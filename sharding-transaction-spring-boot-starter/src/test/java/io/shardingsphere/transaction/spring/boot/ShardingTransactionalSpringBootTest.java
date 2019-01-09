/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.transaction.spring.boot;

import io.shardingsphere.core.exception.ShardingException;
import io.shardingsphere.transaction.api.TransactionType;
import io.shardingsphere.transaction.api.TransactionTypeHolder;
import io.shardingsphere.transaction.aspect.ShardingTransactionalAspect;
import io.shardingsphere.transaction.spring.boot.fixture.ShardingTransactionalTestService;
import io.shardingsphere.transaction.spring.boot.util.TransactionManagerMockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = ShardingTransactionalSpringBootTest.class)
@SpringBootApplication
@ComponentScan("io.shardingsphere.transaction.spring.boot.fixture")
public class ShardingTransactionalSpringBootTest {
    
    @Autowired
    private ShardingTransactionalTestService testService;
    
    @Autowired
    private ShardingTransactionalAspect aspect;
    
    private final Statement statement = mock(Statement.class);
    
    private final JpaTransactionManager jpaTransactionManager = mock(JpaTransactionManager.class);
    
    private final DataSourceTransactionManager dataSourceTransactionManager = mock(DataSourceTransactionManager.class);
    
    @Before
    public void setUp() throws SQLException {
        TransactionManagerMockUtil.initTransactionManagerMock(statement, jpaTransactionManager, dataSourceTransactionManager);
    }
    
    @After
    public void tearDown() {
        aspect.setEnvironment(new DataSource[]{});
    }
    
    @Test
    public void assertChangeTransactionTypeToXA() {
        testService.testChangeTransactionTypeToXA();
        assertThat(TransactionTypeHolder.get(), is(TransactionType.LOCAL));
    }
    
    @Test
    public void assertChangeTransactionTypeToBASE() {
        testService.testChangeTransactionTypeToBASE();
        assertThat(TransactionTypeHolder.get(), is(TransactionType.LOCAL));
    }
    
    @Test
    public void assertChangeTransactionTypeToLocal() {
        TransactionTypeHolder.set(TransactionType.XA);
        testService.testChangeTransactionTypeToLOCAL();
        assertThat(TransactionTypeHolder.get(), is(TransactionType.LOCAL));
    }
    
    @Test
    public void assertChangeTransactionTypeInClass() {
        testService.testChangeTransactionTypeInClass();
        assertThat(TransactionTypeHolder.get(), is(TransactionType.LOCAL));
    }
    
    @Test(expected = ShardingException.class)
    public void assertChangeTransactionTypeForProxyWithIllegalTransactionManager() throws SQLException {
        TransactionManagerMockUtil.testChangeProxyTransactionTypeToLOCAL(testService, aspect, mock(PlatformTransactionManager.class));
    }
    
    @Test(expected = ShardingException.class)
    public void assertChangeTransactionTypeForProxyFailed() throws SQLException {
        when(statement.execute(anyString())).thenThrow(new SQLException("test switch exception"));
        TransactionManagerMockUtil.testChangeProxyTransactionTypeToLOCAL(testService, aspect, dataSourceTransactionManager);
    }
    
    @Test
    public void assertChangeTransactionTypeToLOCALForProxy() throws SQLException {
        when(statement.execute(anyString())).thenReturn(true);
        TransactionManagerMockUtil.testChangeProxyTransactionTypeToLOCAL(testService, aspect, dataSourceTransactionManager);
        TransactionManagerMockUtil.testChangeProxyTransactionTypeToLOCAL(testService, aspect, jpaTransactionManager);
        verify(statement, times(2)).execute("SCTL:SET TRANSACTION_TYPE=LOCAL");
    }
    
    @Test
    public void assertChangeTransactionTypeToXAForProxy() throws SQLException {
        when(statement.execute(anyString())).thenReturn(true);
        TransactionManagerMockUtil.testChangeProxyTransactionTypeToXA(testService, aspect, dataSourceTransactionManager);
        TransactionManagerMockUtil.testChangeProxyTransactionTypeToXA(testService, aspect, jpaTransactionManager);
        verify(statement, times(2)).execute("SCTL:SET TRANSACTION_TYPE=XA");
    }
    
    @Test
    public void assertChangeTransactionTypeToBASEForProxy() throws SQLException {
        when(statement.execute(anyString())).thenReturn(true);
        TransactionManagerMockUtil.testChangeProxyTransactionTypeToBASE(testService, aspect, dataSourceTransactionManager);
        TransactionManagerMockUtil.testChangeProxyTransactionTypeToBASE(testService, aspect, jpaTransactionManager);
        verify(statement, times(2)).execute("SCTL:SET TRANSACTION_TYPE=BASE");
    }
}
