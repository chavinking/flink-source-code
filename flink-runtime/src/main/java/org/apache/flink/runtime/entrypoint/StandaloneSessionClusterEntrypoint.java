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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

/** Entry point for the standalone session cluster. */
public class StandaloneSessionClusterEntrypoint extends SessionClusterEntrypoint {

    public StandaloneSessionClusterEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected DefaultDispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration) {
        return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
                StandaloneResourceManagerFactory.getInstance());
    }

    /**
     * StandaloneSession集群启动类
     * 启动路径：start-cluster.sh --> jobmanager.sh --> flink-deamon.sh start host port
     *
     * @param args
     */
    public static void main(String[] args) {
        // startup checks and logging
//         打印版本信息
        EnvironmentInformation.logEnvironmentInfo(LOG, StandaloneSessionClusterEntrypoint.class.getSimpleName(), args);
//        注册一些信号处理
        SignalHandler.register(LOG);
//        注册钩子
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

//        解析命令行参数
        final EntrypointClusterConfiguration entrypointClusterConfiguration =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new EntrypointClusterConfigurationParserFactory(),
                        StandaloneSessionClusterEntrypoint.class
                );

//        解析配置文件参数
        Configuration configuration = loadConfiguration(entrypointClusterConfiguration);

//        传入配置参数，创建集群组件对象
        StandaloneSessionClusterEntrypoint entrypoint = new StandaloneSessionClusterEntrypoint(configuration);

        // 启动组件
        ClusterEntrypoint.runClusterEntrypoint(entrypoint);
    }
}
