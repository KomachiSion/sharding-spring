/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.shardingsphere.shardingjdbc.orchestration.spring.boot;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.shardingsphere.shardingjdbc.orchestration.spring.boot.common.SpringBootConfigMapConfigurationProperties;
import io.shardingsphere.shardingjdbc.orchestration.spring.boot.common.SpringBootPropertiesConfigurationProperties;
import io.shardingsphere.shardingjdbc.orchestration.spring.boot.masterslave.SpringBootMasterSlaveRuleConfigurationProperties;
import io.shardingsphere.shardingjdbc.orchestration.spring.boot.orchestration.SpringBootOrchestrationConfigurationProperties;
import io.shardingsphere.shardingjdbc.orchestration.spring.boot.sharding.SpringBootShardingRuleConfigurationProperties;
import io.shardingsphere.shardingjdbc.orchestration.spring.boot.util.PropertyUtil;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.constant.ShardingConstant;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.util.InlineExpressionParser;
import org.apache.shardingsphere.orchestration.internal.registry.ShardingOrchestrationFacade;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.MasterSlaveDataSource;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.ShardingDataSource;
import org.apache.shardingsphere.shardingjdbc.orchestration.internal.datasource.OrchestrationMasterSlaveDataSource;
import org.apache.shardingsphere.shardingjdbc.orchestration.internal.datasource.OrchestrationShardingDataSource;
import org.apache.shardingsphere.shardingjdbc.util.DataSourceUtil;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Orchestration spring boot sharding and master-slave configuration.
 *
 * @author caohao
 * @author panjuan
 */
@Configuration
@EnableConfigurationProperties({
        SpringBootShardingRuleConfigurationProperties.class, SpringBootMasterSlaveRuleConfigurationProperties.class,
        SpringBootConfigMapConfigurationProperties.class, SpringBootPropertiesConfigurationProperties.class, 
        SpringBootOrchestrationConfigurationProperties.class})
@RequiredArgsConstructor
public class OrchestrationSpringBootConfiguration implements EnvironmentAware {
    
    private final Map<String, DataSource> dataSourceMap = new LinkedHashMap<>();
    
    private final SpringBootShardingRuleConfigurationProperties shardingProperties;
    
    private final SpringBootMasterSlaveRuleConfigurationProperties masterSlaveProperties;
    
    private final SpringBootConfigMapConfigurationProperties configMapProperties;
    
    private final SpringBootPropertiesConfigurationProperties propProperties;
    
    private final SpringBootOrchestrationConfigurationProperties orchestrationProperties;
    
    /**
     * Get data source bean.
     * 
     * @return data source bean
     * @throws SQLException SQL Exception
     */
    @Bean
    public DataSource dataSource() throws SQLException {
        Preconditions.checkState(isValidConfiguration(), "The orchestration configuration is invalid, please choose one from Sharding rule and Master-slave rule.");
        return isShardingRule() ? createShardingDataSource() : createMasterSlaveDataSource();
    }
    
    private boolean isValidConfiguration() {
        return isValidRuleConfiguration() || isValidOrchestrationConfiguration();
    }
    
    private boolean isValidRuleConfiguration() {
        return (shardingProperties.getTables().isEmpty() && !Strings.isNullOrEmpty(masterSlaveProperties.getMasterDataSourceName()))
                || (!shardingProperties.getTables().isEmpty() && Strings.isNullOrEmpty(masterSlaveProperties.getMasterDataSourceName()));
    }
    
    private boolean isValidOrchestrationConfiguration() {
        return !Strings.isNullOrEmpty(orchestrationProperties.getName());
    }
    
    private boolean isShardingRule() {
        return isValidRuleConfiguration() ? isShardingRuleByLocal() : isShardingRuleByRegistry();
    }
    
    private boolean isShardingRuleByLocal() {
        return !shardingProperties.getTables().isEmpty();
    }
    
    private boolean isShardingRuleByRegistry() {
        try (ShardingOrchestrationFacade shardingOrchestrationFacade = new ShardingOrchestrationFacade(
                orchestrationProperties.getOrchestrationConfiguration(), Collections.singletonList(ShardingConstant.LOGIC_SCHEMA_NAME))) {
            return shardingOrchestrationFacade.getConfigService().isShardingRule(ShardingConstant.LOGIC_SCHEMA_NAME);
        }
    }
    
    private DataSource createShardingDataSource() throws SQLException {
        if (shardingProperties.getTables().isEmpty()) {
            return new OrchestrationShardingDataSource(orchestrationProperties.getOrchestrationConfiguration());
        }
        ShardingDataSource shardingDataSource = new ShardingDataSource(
                dataSourceMap, new ShardingRule(shardingProperties.getShardingRuleConfiguration(), dataSourceMap.keySet()), configMapProperties.getConfigMap(), propProperties.getProps());
        return new OrchestrationShardingDataSource(shardingDataSource, orchestrationProperties.getOrchestrationConfiguration());
    }
    
    private DataSource createMasterSlaveDataSource() throws SQLException {
        if (Strings.isNullOrEmpty(masterSlaveProperties.getMasterDataSourceName())) {
            return new OrchestrationMasterSlaveDataSource(orchestrationProperties.getOrchestrationConfiguration());
        }
        MasterSlaveDataSource masterSlaveDataSource = new MasterSlaveDataSource(
                dataSourceMap, masterSlaveProperties.getMasterSlaveRuleConfiguration(), configMapProperties.getConfigMap(), propProperties.getProps());
        return new OrchestrationMasterSlaveDataSource(masterSlaveDataSource, orchestrationProperties.getOrchestrationConfiguration());
    }
    
    @Override
    public final void setEnvironment(final Environment environment) {
        String prefix = "sharding.jdbc.datasource.";
        for (String each : getDataSourceNames(environment, prefix)) {
            try {
                dataSourceMap.put(each, getDataSource(environment, prefix, each));
            } catch (final ReflectiveOperationException ex) {
                throw new ShardingException("Can't find datasource type!", ex);
            }
        }
    }
    
    private List<String> getDataSourceNames(final Environment environment, final String prefix) {
        StandardEnvironment standardEnv = (StandardEnvironment) environment;
        standardEnv.setIgnoreUnresolvableNestedPlaceholders(true);
        String dataSources = standardEnv.getProperty(prefix + "names");
        if (StringUtils.isEmpty(dataSources)) {
            return Collections.emptyList();
        }
        return new InlineExpressionParser(dataSources).splitAndEvaluate();
    }
    
    @SuppressWarnings("unchecked")
    private DataSource getDataSource(final Environment environment, final String prefix, final String dataSourceName) throws ReflectiveOperationException {
        Map<String, Object> dataSourceProps = PropertyUtil.handle(environment, prefix + dataSourceName, Map.class);
        Preconditions.checkState(!dataSourceProps.isEmpty(), String.format("Wrong datasource [%s] properties!", dataSourceName));
        return DataSourceUtil.getDataSource(dataSourceProps.get("type").toString(), dataSourceProps);
    }
}
