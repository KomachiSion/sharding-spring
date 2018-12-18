# Sharding-Spring

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

[![Build Status](https://api.travis-ci.org/sharding-sphere/sharding-spring.png?branch=master)](https://travis-ci.org/sharding-sphere/sharding-sphere)

## 概述

Sharding-Spring是[ShardingSphere](http://shardingsphere.io/index_zh.html)的Spring集成插件。
旨在减少Spring框架中使用ShardingSphere的成本。

### Sharding-jdbc-spring

Sharding-jdbc-spring为Sharding-JDBC提供了Spring-Boot自动装配和Spring的命名空间，以减少用户在Spring中使用ShardingSphere的配置内容。

### Sharding-jdbc-orchestration-spring

Sharding-jdbc-orchestration-spring在sharding-jdbc-spring的基础上，添加了关于数据治理的自动装配和命名空间内容。

### Sharding-transaction-spring

Sharding-transaction-spring拓展了Spring的`@Transactional`注解，当使用ShardingSphere的分布式事务时，可以通过新注解在不同类型的事务类型中进行切换。