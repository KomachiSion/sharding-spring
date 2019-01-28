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

package io.shardingsphere.shardingjdbc.spring.namespace.parser;

import com.google.common.base.Strings;
import io.shardingsphere.shardingjdbc.spring.namespace.constants.ShardingDataSourceBeanDefinitionParserTag;
import org.apache.shardingsphere.api.config.KeyGeneratorConfiguration;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * Key generator bean parser for spring namespace.
 *®
 * @author panjuan
 */
public final class KeyGeneratorBeanDefinitionParser extends AbstractBeanDefinitionParser {
    
    @Override
    protected AbstractBeanDefinition parseInternal(final Element element, final ParserContext parserContext) {
        BeanDefinitionBuilder factory = BeanDefinitionBuilder.rootBeanDefinition(KeyGeneratorConfiguration.class);
        factory.addPropertyValue("column", element.getAttribute(ShardingDataSourceBeanDefinitionParserTag.GENERATE_KEY_COLUMN_ATTRIBUTE));
        factory.addPropertyValue("type", element.getAttribute(ShardingDataSourceBeanDefinitionParserTag.GENERATE_KEY_TYPE_ATTRIBUTE));
        String properties = element.getAttribute(ShardingDataSourceBeanDefinitionParserTag.GENERATE_KEY_PROPERTY_REF_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(properties)) {
            factory.addPropertyReference("props", properties);
        }
        return factory.getBeanDefinition();
    }
}
