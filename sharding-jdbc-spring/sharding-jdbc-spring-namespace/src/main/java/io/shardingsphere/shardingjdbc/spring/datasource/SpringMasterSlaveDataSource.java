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

package io.shardingsphere.shardingjdbc.spring.datasource;

import org.apache.shardingsphere.api.algorithm.masterslave.MasterSlaveLoadBalanceAlgorithm;
import org.apache.shardingsphere.api.algorithm.masterslave.MasterSlaveLoadBalanceAlgorithmType;
import org.apache.shardingsphere.api.config.rule.MasterSlaveRuleConfiguration;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.MasterSlaveDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Master-slave datasource for spring namespace.
 *
 * @author zhangliang
 */
public class SpringMasterSlaveDataSource extends MasterSlaveDataSource {
    
    public SpringMasterSlaveDataSource(final Map<String, DataSource> dataSourceMap, final String name,
                                       final String masterDataSourceName, final Collection<String> slaveDataSourceNames, final MasterSlaveLoadBalanceAlgorithm strategy, 
                                       final Map<String, Object> configMap, final Properties props) throws SQLException {
        super(dataSourceMap, getMasterSlaveRuleConfiguration(name, masterDataSourceName, slaveDataSourceNames, strategy),
                null == configMap ? new LinkedHashMap<String, Object>() : configMap, null == props ? new Properties() : props);
    }
    
    public SpringMasterSlaveDataSource(final Map<String, DataSource> dataSourceMap, final String name,
                                       final String masterDataSourceName, final Collection<String> slaveDataSourceNames, final MasterSlaveLoadBalanceAlgorithmType strategyType, 
                                       final Map<String, Object> configMap, final Properties props) throws SQLException {
        super(dataSourceMap,
                getMasterSlaveRuleConfiguration(name, masterDataSourceName, slaveDataSourceNames, null == strategyType ? null : strategyType.getAlgorithm()),
                null == configMap ? new LinkedHashMap<String, Object>() : configMap, null == props ? new Properties() : props);
    }
    
    private static MasterSlaveRuleConfiguration getMasterSlaveRuleConfiguration(
            final String name, final String masterDataSourceName, final Collection<String> slaveDataSourceNames, final MasterSlaveLoadBalanceAlgorithm loadBalanceAlgorithm) {
        return new MasterSlaveRuleConfiguration(name, masterDataSourceName, slaveDataSourceNames, loadBalanceAlgorithm);
    }
}
