package day08;

import Utils.Flink_Local_Connection;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 *
 * 自定义一个source,并且使用OperatorState记录偏移量,可以实现AtLeastOnce
 *
 * 互相
 *
 */
public class demo01_AtLeastOnceFileSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = Flink_Local_Connection.getConnection();
        env.setParallelism(4);
        env.enableCheckpointing(5000);
        DataStreamSource<String> source = env.addSource(new AtLeastOnceFileSource("d:\\g\\gdata"));
        source.print();
        env.execute();
    }


    /**
     * 使用OperatorState要实现一个接口
     *
     * 先执行 initializeState -> open -> run(一直运行)
     * snapshotState在checkPoint时, 每一次次CheckPoint时,都会调用一次snapshotState
     *
     *
     *
     */
    public static class AtLeastOnceFileSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
    private boolean flag = true;
        //transient瞬时的,修饰的变量不参与序列化和反序列化
    private transient ListState<Long> offsetState;
    private  String path;
    private long offset;
    private AtLeastOnceFileSource(String path){
        this.path =path;

    }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // System.out.println("initializeState");
            //定义状态描述器
            ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<>("offect-state", Long.class);
            //获取OperatorState中的ListState
            offsetState = context.getOperatorStateStore().getListState(listStateDescriptor);
            //将offsetState偏移量恢复
            if (context.isRestored()) {  //判断OperatorState是否恢复完成
                for (Long offset : offsetState.get()) {
                    this.offset =offset;

                }
            }

        }
        @Override
        public void open(Configuration parameters) throws Exception {
           // System.out.println("l");
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            RandomAccessFile randomAccessFile = new RandomAccessFile(path + "\\" + indexOfThisSubtask +".txt","r");
            randomAccessFile.seek(offset);//获取当前的偏移量
            while (flag) {
                String line = randomAccessFile.readLine();
                if (line != null) {
                    line = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
                    ctx.collect(indexOfThisSubtask + ":" + line);
                    //记录新的偏移量
                    offset = randomAccessFile.getFilePointer();
                } else {
                    Thread.sleep(500);
                }
            }

        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            offsetState.clear();
            offsetState.add(offset);
        }

        @Override
        public void cancel() {
        flag = false;
            //System.out.println("cancel");
        }




    }
}
