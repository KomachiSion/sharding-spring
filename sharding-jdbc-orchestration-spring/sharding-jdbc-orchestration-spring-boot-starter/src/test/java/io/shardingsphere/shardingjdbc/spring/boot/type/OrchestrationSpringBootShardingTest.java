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

package io.shardingsphere.shardingjdbc.spring.boot.type;

import io.shardingsphere.shardingjdbc.spring.boot.util.EmbedTestingServer;
import lombok.SneakyThrows;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.shardingsphere.api.ConfigMapContext;
import org.apache.shardingsphere.core.constant.properties.ShardingProperties;
import org.apache.shardingsphere.core.constant.properties.ShardingPropertiesConstant;
import org.apache.shardingsphere.core.routing.strategy.inline.InlineShardingStrategy;
import org.apache.shardingsphere.core.rule.DataNode;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.TableRule;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.ShardingContext;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.ShardingDataSource;
import org.apache.shardingsphere.shardingjdbc.orchestration.internal.datasource.OrchestrationShardingDataSource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = OrchestrationSpringBootShardingTest.class)
@SpringBootApplication
@ActiveProfiles("sharding")
public class OrchestrationSpringBootShardingTest {
    
    @Resource
    private DataSource dataSource;
    
    @BeforeClass
    public static void init() {
        EmbedTestingServer.start();
        ConfigMapContext.getInstance().getConfigMap().clear();
    }
    
    @Test
    public void assertWithShardingDataSource() {
        assertTrue(dataSource instanceof OrchestrationShardingDataSource);
        ShardingDataSource shardingDataSource = getFieldValue("dataSource", OrchestrationShardingDataSource.class, dataSource);
        ShardingContext shardingContext = getFieldValue("shardingContext", ShardingDataSource.class, shardingDataSource);
        for (DataSource each : shardingDataSource.getDataSourceMap().values()) {
            assertThat(((BasicDataSource) each).getMaxTotal(), is(16));
        }
        assertTrue(shardingContext.getShardingProperties().<Boolean>getValue(ShardingPropertiesConstant.SQL_SHOW));
        Map<String, Object> configMap = new ConcurrentHashMap<>();
        configMap.put("key1", "value1");
        assertThat(ConfigMapContext.getInstance().getConfigMap(), is(configMap));
        ShardingProperties shardingProperties = shardingContext.getShardingProperties();
        assertTrue((Boolean) shardingProperties.getValue(ShardingPropertiesConstant.SQL_SHOW));
        assertThat((Integer) shardingProperties.getValue(ShardingPropertiesConstant.EXECUTOR_SIZE), is(100));
    }
    
    @Test
    public void assertWithShardingDataSourceNames() {
        ShardingDataSource shardingDataSource = getFieldValue("dataSource", OrchestrationShardingDataSource.class, dataSource);
        ShardingContext shardingContext = getFieldValue("shardingContext", ShardingDataSource.class, shardingDataSource);
        ShardingRule shardingRule = shardingContext.getShardingRule();
        assertThat(shardingRule.getShardingDataSourceNames().getDataSourceNames().size(), is(3));
        assertTrue(shardingRule.getShardingDataSourceNames().getDataSourceNames().contains("ds"));
        assertTrue(shardingRule.getShardingDataSourceNames().getDataSourceNames().contains("ds_0"));
        assertTrue(shardingRule.getShardingDataSourceNames().getDataSourceNames().contains("ds_1"));
    }
    
    @Test
    public void assertWithTableRules() {
        ShardingDataSource shardingDataSource = getFieldValue("dataSource", OrchestrationShardingDataSource.class, dataSource);
        ShardingContext shardingContext = getFieldValue("shardingContext", ShardingDataSource.class, shardingDataSource);
        ShardingRule shardingRule = shardingContext.getShardingRule();
        assertThat(shardingRule.getTableRules().size(), is(2));
        TableRule tableRule1 = (new LinkedList<>(shardingRule.getTableRules())).get(0);
        assertThat(tableRule1.getLogicTable(), is("t_order_item"));
        assertThat(tableRule1.getActualDataNodes().size(), is(4));
        assertTrue(tableRule1.getActualDataNodes().contains(new DataNode("ds_0", "t_order_item_0")));
        assertTrue(tableRule1.getActualDataNodes().contains(new DataNode("ds_0", "t_order_item_1")));
        assertTrue(tableRule1.getActualDataNodes().contains(new DataNode("ds_1", "t_order_item_0")));
        assertTrue(tableRule1.getActualDataNodes().contains(new DataNode("ds_1", "t_order_item_1")));
        assertThat(tableRule1.getTableShardingStrategy(), instanceOf(InlineShardingStrategy.class));
        assertThat(tableRule1.getTableShardingStrategy().getShardingColumns().iterator().next(), is("order_id"));
        assertThat(tableRule1.getGenerateKeyColumn(), is("order_item_id"));
        TableRule tableRule2 = (new LinkedList<>(shardingRule.getTableRules())).get(1);
        assertThat(tableRule2.getLogicTable(), is("t_order"));
        assertThat(tableRule2.getActualDataNodes().size(), is(4));
        assertTrue(tableRule2.getActualDataNodes().contains(new DataNode("ds_0", "t_order_0")));
        assertTrue(tableRule2.getActualDataNodes().contains(new DataNode("ds_0", "t_order_1")));
        assertTrue(tableRule2.getActualDataNodes().contains(new DataNode("ds_1", "t_order_0")));
        assertTrue(tableRule2.getActualDataNodes().contains(new DataNode("ds_1", "t_order_1")));
        assertThat(tableRule1.getTableShardingStrategy(), instanceOf(InlineShardingStrategy.class));
        assertThat(tableRule1.getTableShardingStrategy().getShardingColumns().iterator().next(), is("order_id"));
        assertThat(tableRule2.getGenerateKeyColumn(), is("order_id"));
    }
    
    @Test
    public void assertWithBindingTableRules() {
        ShardingDataSource shardingDataSource = getFieldValue("dataSource", OrchestrationShardingDataSource.class, dataSource);
        ShardingContext shardingContext = getFieldValue("shardingContext", ShardingDataSource.class, shardingDataSource);
        ShardingRule shardingRule = shardingContext.getShardingRule();
        assertThat(shardingRule.getBindingTableRules().size(), is(2));
        TableRule tableRule1 = (new LinkedList<>(shardingRule.getTableRules())).get(0);
        assertThat(tableRule1.getLogicTable(), is("t_order_item"));
        assertThat(tableRule1.getActualDataNodes().size(), is(4));
        assertTrue(tableRule1.getActualDataNodes().contains(new DataNode("ds_0", "t_order_item_0")));
        assertTrue(tableRule1.getActualDataNodes().contains(new DataNode("ds_0", "t_order_item_1")));
        assertTrue(tableRule1.getActualDataNodes().contains(new DataNode("ds_1", "t_order_item_0")));
        assertTrue(tableRule1.getActualDataNodes().contains(new DataNode("ds_1", "t_order_item_1")));
        assertThat(tableRule1.getTableShardingStrategy(), instanceOf(InlineShardingStrategy.class));
        assertThat(tableRule1.getTableShardingStrategy().getShardingColumns().iterator().next(), is("order_id"));
        assertThat(tableRule1.getGenerateKeyColumn(), is("order_item_id"));
        TableRule tableRule2 = (new LinkedList<>(shardingRule.getTableRules())).get(1);
        assertThat(tableRule2.getLogicTable(), is("t_order"));
        assertThat(tableRule2.getActualDataNodes().size(), is(4));
        assertTrue(tableRule2.getActualDataNodes().contains(new DataNode("ds_0", "t_order_0")));
        assertTrue(tableRule2.getActualDataNodes().contains(new DataNode("ds_0", "t_order_1")));
        assertTrue(tableRule2.getActualDataNodes().contains(new DataNode("ds_1", "t_order_0")));
        assertTrue(tableRule2.getActualDataNodes().contains(new DataNode("ds_1", "t_order_1")));
        assertThat(tableRule1.getTableShardingStrategy(), instanceOf(InlineShardingStrategy.class));
        assertThat(tableRule1.getTableShardingStrategy().getShardingColumns().iterator().next(), is("order_id"));
        assertThat(tableRule2.getGenerateKeyColumn(), is("order_id"));
    }
    
    @Test
    public void assertWithBroadcastTables() {
        ShardingDataSource shardingDataSource = getFieldValue("dataSource", OrchestrationShardingDataSource.class, dataSource);
        ShardingContext shardingContext = getFieldValue("shardingContext", ShardingDataSource.class, shardingDataSource);
        ShardingRule shardingRule = shardingContext.getShardingRule();
        assertThat(shardingRule.getBroadcastTables().size(), is(1));
        assertThat(shardingRule.getBroadcastTables().iterator().next(), is("t_config"));
    }
    
    @SuppressWarnings("unchecked")
    @SneakyThrows
    private <T> T getFieldValue(final String fieldName, final Class<?> fieldClass, final Object target) {
        Field field = fieldClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }
}
