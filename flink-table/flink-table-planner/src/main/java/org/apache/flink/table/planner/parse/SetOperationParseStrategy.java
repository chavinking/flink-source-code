/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.parse;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.SetOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Strategy to parse statement to {@link SetOperation}. */
public class SetOperationParseStrategy extends AbstractRegexParseStrategy {

    static final SetOperationParseStrategy INSTANCE = new SetOperationParseStrategy();

    protected SetOperationParseStrategy() {
        super(
//                用于匹配 set key=value; 模式代码
                Pattern.compile(
                        "SET(\\s+(?<key>[^'\\s]+)\\s*=\\s*('(?<quotedVal>[^']*)'|(?<val>[^;\\s]+)))?\\s*;?",
                        DEFAULT_PATTERN_FLAGS));
    }

//    转换SET语句为Operation
    @Override
    public Operation convert(String statement) {
        // 匹配statement
        Matcher matcher = pattern.matcher(statement.trim());

        // 创建保存操作符的集合
        final List<String> operands = new ArrayList<>();
        if (matcher.find()) {
            if (matcher.group("key") != null) {
                // 获取key和对应的quotedVal或者val，加入操作符集合
                operands.add(matcher.group("key"));
                operands.add(
                        matcher.group("quotedVal") != null
                                ? matcher.group("quotedVal")
                                : matcher.group("val")
                );
            }
        }

        // only capture SET
        // 如果只有SET个单词，operands为空，创建一个空的SetOperation
        if (operands.isEmpty()) {
            return new SetOperation();
        } else if (operands.size() == 2) {
            // 如果operands的大小为2，说明解析到了key和val或者quotedVal，创建SetOperation
            return new SetOperation(operands.get(0), operands.get(1));
        } else {
            // impossible
            // 其他情况，抛出异常
            throw new TableException(
                    String.format(
                            "Failed to convert the statement to SET operation: %s.", statement));
        }
    }



    @Override
    public String[] getHints() {
        return new String[] {"SET"};
    }
}
