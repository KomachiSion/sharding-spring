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

package io.shardingsphere.shardingjdbc.spring;

import io.shardingsphere.shardingjdbc.spring.util.FieldValueUtil;
import org.apache.shardingsphere.api.ConfigMapContext;
import org.apache.shardingsphere.api.algorithm.masterslave.MasterSlaveLoadBalanceAlgorithm;
import org.apache.shardingsphere.api.algorithm.masterslave.RandomMasterSlaveLoadBalanceAlgorithm;
import org.apache.shardingsphere.api.algorithm.masterslave.RoundRobinMasterSlaveLoadBalanceAlgorithm;
import org.apache.shardingsphere.core.rule.MasterSlaveRule;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.MasterSlaveDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@ContextConfiguration(locations = "classpath:META-INF/rdb/masterSlaveNamespace.xml")
public class MasterSlaveNamespaceTest extends AbstractJUnit4SpringContextTests {
    
    @BeforeClass
    public static void setUp() {
        ConfigMapContext.getInstance().getConfigMap().clear();
    }
    
    @Test
    public void assertDefaultMaserSlaveDataSource() {
        MasterSlaveRule masterSlaveRule = getMasterSlaveRule("defaultMasterSlaveDataSource");
        assertThat(masterSlaveRule.getMasterDataSourceName(), is("dbtbl_0_master"));
        assertTrue(masterSlaveRule.getSlaveDataSourceNames().contains("dbtbl_0_slave_0"));
        assertTrue(masterSlaveRule.getSlaveDataSourceNames().contains("dbtbl_0_slave_1"));
    }
    
    @Test
    public void assertTypeMasterSlaveDataSource() {
        MasterSlaveRule randomSlaveRule = getMasterSlaveRule("randomMasterSlaveDataSource");
        MasterSlaveRule roundRobinSlaveRule = getMasterSlaveRule("roundRobinMasterSlaveDataSource");
        assertTrue(randomSlaveRule.getLoadBalanceAlgorithm() instanceof RandomMasterSlaveLoadBalanceAlgorithm);
        assertTrue(roundRobinSlaveRule.getLoadBalanceAlgorithm() instanceof RoundRobinMasterSlaveLoadBalanceAlgorithm);
    }
    
    @Test
    public void assertRefMasterSlaveDataSource() {
        MasterSlaveLoadBalanceAlgorithm randomStrategy = this.applicationContext.getBean("randomStrategy", MasterSlaveLoadBalanceAlgorithm.class);
        MasterSlaveRule masterSlaveRule = getMasterSlaveRule("refMasterSlaveDataSource");
        assertTrue(masterSlaveRule.getLoadBalanceAlgorithm() == randomStrategy);
    }
    
    private MasterSlaveRule getMasterSlaveRule(final String masterSlaveDataSourceName) {
        MasterSlaveDataSource masterSlaveDataSource = this.applicationContext.getBean(masterSlaveDataSourceName, MasterSlaveDataSource.class);
        return (MasterSlaveRule) FieldValueUtil.getFieldValue(masterSlaveDataSource, "masterSlaveRule", true);
    }
    
    @Test
    public void assertConfigMapDataSource() {
        Object masterSlaveDataSource = this.applicationContext.getBean("configMapDataSource");
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("key1", "value1");
        assertThat(ConfigMapContext.getInstance().getConfigMap(), is(configMap));
        assertThat(masterSlaveDataSource, instanceOf(MasterSlaveDataSource.class));
    }
}
