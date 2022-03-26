package com.knn3.rt.scene.ifcondition.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author apophis
 * @File JobUtils
 * @Time 2022/3/25 13:06
 * @Description 工程描述
 */
@Slf4j
public class JobUtils {
    public static StreamExecutionEnvironment getEnv(String[] args) throws Exception {
        JobUtils.log.info("添加命令行参数: --mode local|cluster --properties 路径");
        StreamExecutionEnvironment env;
        ParameterTool params;
        ParameterTool argsParam = ParameterTool.fromArgs(args);
        String mode = argsParam.get(Param.MODE, Mode.CLUSTER);
        String propertiesFile = argsParam.get(Param.PROPERTIES_FILE);
        switch (mode) {
            case Mode.LOCAL:
                params = ParameterTool.fromPropertiesFile(propertiesFile);
                env = StreamExecutionEnvironment.createLocalEnvironment();
                break;
            case Mode.CLUSTER:
                params = ParameterTool.fromPropertiesFile(propertiesFile);
                env = StreamExecutionEnvironment.getExecutionEnvironment();
                StateBackend backend;
                String checkpointUrl = argsParam.getRequired(Param.CHECKPOINT_URL);
                String backEnd = argsParam.get(Param.BACKEND);

                if (backEnd.equals(Backend.ROCKSDB)) backend = new RocksDBStateBackend(checkpointUrl, true);
                else if (backEnd.equals(Backend.FS)) backend = new FsStateBackend(checkpointUrl, true);
                else throw new Exception("StateBackend is err");
                env.setStateBackend(backend);
                break;
            default:
                throw new RuntimeException("env设置错误");
        }
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.minutes(1)));
        env.enableCheckpointing(6 * 60 * 1000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(6 * 60 * 1000);
        env.getCheckpointConfig().setCheckpointTimeout(6 * 60 * 1000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        if (argsParam.getNumberOfParameters() != 0) params = params.mergeWith(argsParam);
        env.getConfig().setUseSnapshotCompression(true);
        env.getConfig().setGlobalJobParameters(params);
        return env;
    }

    /**
     * 命令参数
     */
    private interface Param {
        String MODE = "mode";
        String PROPERTIES_FILE = "properties";
        String CHECKPOINT_URL = "checkpointUrl";
        String BACKEND = "backend";
    }

    /**
     * 状态后端
     */
    private interface Backend {
        String FS = "fs";
        String ROCKSDB = "rocksdb";
    }

    /**
     * 运行模式
     */
    private interface Mode {
        String LOCAL = "local";
        String CLUSTER = "cluster";
    }
}
