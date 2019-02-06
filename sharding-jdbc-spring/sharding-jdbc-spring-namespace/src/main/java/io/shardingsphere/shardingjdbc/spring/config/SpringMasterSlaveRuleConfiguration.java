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

package io.shardingsphere.shardingjdbc.spring.config;

import lombok.Getter;
import org.apache.shardingsphere.api.config.masterslave.LoadBalanceStrategyConfiguration;
import org.apache.shardingsphere.api.config.masterslave.MasterSlaveRuleConfiguration;
import org.apache.shardingsphere.spi.algorithm.masterslave.MasterSlaveLoadBalanceAlgorithm;

import java.util.Collection;

/**
 * Master-slave rule configuration for spring namespace.
 *
 * @author zhangliang
 */
@Getter
public final class SpringMasterSlaveRuleConfiguration extends MasterSlaveRuleConfiguration {
    
    private final MasterSlaveLoadBalanceAlgorithm loadBalanceAlgorithm;
    
    public SpringMasterSlaveRuleConfiguration(final String name, final String masterDataSourceName, final Collection<String> slaveDataSourceNames, 
                                              final MasterSlaveLoadBalanceAlgorithm loadBalanceAlgorithm) {
        super(name, masterDataSourceName, slaveDataSourceNames, 
                null == loadBalanceAlgorithm ? null : new LoadBalanceStrategyConfiguration(loadBalanceAlgorithm.getType(), loadBalanceAlgorithm.getProperties()));
        this.loadBalanceAlgorithm = loadBalanceAlgorithm;
    }
}
