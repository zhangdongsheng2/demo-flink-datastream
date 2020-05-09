package com.commerce.commons.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static com.commerce.commons.constant.PropertiesConstants.*;


/**
 * Desc: Checkpoint 工具类
 */
public class CheckPointUtil {

    public static StreamExecutionEnvironment setCheckpointConfig(StreamExecutionEnvironment env, ParameterTool parameterTool) throws Exception {
        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false) && CHECKPOINT_MEMORY.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
            //1、state 存放在内存中，默认是 5M
            StateBackend stateBackend = new MemoryStateBackend(5 * 1024 * 1024 * 100);
            env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, TimeUnit.MINUTES.toMillis(10)));
            env.setStateBackend(stateBackend);
        }

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false) && CHECKPOINT_FS.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
            StateBackend stateBackend = new FsStateBackend(new URI(parameterTool.get(STREAM_CHECKPOINT_DIR)), 0);
            env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, TimeUnit.MINUTES.toMillis(10)));
            env.setStateBackend(stateBackend);
        }

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false) && CHECKPOINT_ROCKETSDB.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
            RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(parameterTool.get(STREAM_CHECKPOINT_DIR));
            env.setStateBackend(rocksDBStateBackend);
        }


        // 配置 Checkpoint
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        // make sure 8 分钟 of progress happen between checkpoints
        checkpointConf.setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(8));
        // 设置 checkpoint 必须在20分钟内完成，否则会被丢弃
        checkpointConf.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(20));
        //当程序关闭的时候，会触发额外的checkpoints
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置 checkpoint 失败时，任务不会 fail，该 checkpoint 会被丢弃
        //        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        //        checkpointConf.setTolerableCheckpointFailureNumber();

        return env;
    }
}
