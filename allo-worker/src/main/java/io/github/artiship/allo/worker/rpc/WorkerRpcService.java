package io.github.artiship.allo.worker.rpc;

import com.google.common.base.Throwables;
import io.github.artiship.allo.model.bo.TaskBo;
import io.github.artiship.allo.rpc.RpcUtils;
import io.github.artiship.allo.rpc.api.HealthCheckRequest;
import io.github.artiship.allo.rpc.api.HealthCheckResponse;
import io.github.artiship.allo.rpc.api.RpcResponse;
import io.github.artiship.allo.rpc.api.RpcTask;
import io.github.artiship.allo.rpc.api.SchedulerServiceGrpc.SchedulerServiceImplBase;
import io.github.artiship.allo.storage.SharedStorage;
import io.github.artiship.allo.worker.common.CommonCache;
import io.github.artiship.allo.worker.executor.LocalExecutor;
import io.github.com.artiship.ha.TaskStateListener;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** */
@Component
@Slf4j
public class WorkerRpcService extends SchedulerServiceImplBase
        implements InitializingBean, TaskStateListener {

    @Autowired private SharedStorage sharedStorage;

    @Value("${worker.max.task.num}")
    private int maxTaskNums;

    @Value("${worker.task.local.base.path}")
    private String taskLocalBasePath;

    // 任务执行的线程池
    private ExecutorService executorService;

    @Override
    public void afterPropertiesSet() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(maxTaskNums * 3);
    }

    /**
     * 响应master提交任务实现
     *
     * @param rpcTask
     * @param responseObserver
     */
    @Override
    public void submitTask(RpcTask rpcTask, StreamObserver<RpcResponse> responseObserver) {
        log.info("Get submit task request, rpcTask [{}]", rpcTask);
        RpcResponse.Builder rpcResponseBuilder = RpcResponse.newBuilder();
        TaskBo schedulerTaskBo = null;
        try {
            schedulerTaskBo = RpcUtils.toTaskBo(rpcTask);
            submitTask(schedulerTaskBo);
            rpcResponseBuilder.setCode(200);
            // 增加一个任务
            CommonCache.addOneTask();
        } catch (Exception e) {
            rpcResponseBuilder.setCode(500).setMessage(subErrorMsg(e));
            log.error(
                    String.format(
                            "Task_%s_%s, handle submit task request failed.",
                            rpcTask.getJobId(), rpcTask.getId()),
                    e);
        }

        responseObserver.onNext(rpcResponseBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * 响应master KILL任务实现
     *
     * @param rpcTask
     * @param responseObserver
     */
    @Override
    public void killTask(RpcTask rpcTask, StreamObserver<RpcResponse> responseObserver) {
        log.info("Get kill task request, rpcTask [{}]", rpcTask);
        RpcResponse.Builder rpcResponseBuilder = RpcResponse.newBuilder();
        TaskBo schedulerTaskBo = null;
        try {
            schedulerTaskBo = RpcUtils.toTaskBo(rpcTask);
            Future future = killTask(schedulerTaskBo);
            // 等待KILL完成
            future.get();
            rpcResponseBuilder.setCode(200);
        } catch (Exception e) {
            rpcResponseBuilder.setCode(500).setMessage(subErrorMsg(e));
            log.error(
                    String.format(
                            "Task_%s_%s, handle kill task request failed.",
                            rpcTask.getJobId(), rpcTask.getId()),
                    e);
        }

        responseObserver.onNext(rpcResponseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void healthCheck(
            HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        HealthCheckResponse healthCheckResponse =
                HealthCheckResponse.newBuilder()
                        .setStatus(HealthCheckResponse.ServingStatus.SERVING)
                        .build();
        responseObserver.onNext(healthCheckResponse);
        responseObserver.onCompleted();
    }

    /** @param task */
    private void submitTask(TaskBo task) {
        executorService.submit(
                () -> {
                    LocalExecutor executor =
                            new LocalExecutor(task, sharedStorage, taskLocalBasePath);

                    executor.execute();
                });
    }

    /**
     * @param task
     * @return
     */
    public Future killTask(TaskBo task) {
        return null;
    }

    private String subErrorMsg(Exception e) {
        String allErrorMsg = Throwables.getStackTraceAsString(e);
        return allErrorMsg.length() > 50 ? allErrorMsg.substring(0, 50) : allErrorMsg;
    }

    @Override
    public void onRunning(TaskBo task) {}

    @Override
    public void onKilled(TaskBo task) {}

    @Override
    public void onSuccess(TaskBo task) {}

    @Override
    public void onFail(TaskBo task) {}

    @Override
    public void onFailOver(TaskBo task) {}
}
