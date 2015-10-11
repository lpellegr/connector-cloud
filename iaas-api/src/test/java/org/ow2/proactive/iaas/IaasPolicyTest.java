package org.ow2.proactive.iaas;/*
 *  *
 * ProActive Parallel Suite(TM): The Java(TM) library for
 *    Parallel, Distributed, Multi-Core Computing for
 *    Enterprise Grids & Clouds
 *
 * Copyright (C) 1997-2011 INRIA/University of
 *                 Nice-Sophia Antipolis/ActiveEon
 * Contact: proactive@ow2.org or contact@activeeon.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation; version 3 of
 * the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 *
 *  Initial developer(s):               The ProActive Team
 *                        http://proactive.inria.fr/team_members.htm
 *  Contributor(s):
 *
 *  * $$PROACTIVE_INITIAL_DEV$$
 */

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;

import org.objectweb.proactive.core.ProActiveException;
import org.objectweb.proactive.core.node.Node;
import org.objectweb.proactive.core.node.NodeInformation;
import org.ow2.proactive.resourcemanager.common.NodeState;
import org.ow2.proactive.resourcemanager.common.event.RMEventType;
import org.ow2.proactive.resourcemanager.common.event.RMNodeDescriptor;
import org.ow2.proactive.resourcemanager.common.event.RMNodeEvent;
import org.ow2.proactive.resourcemanager.core.RMCore;
import org.ow2.proactive.resourcemanager.nodesource.NodeSource;
import org.ow2.proactive.resourcemanager.utils.RMNodeStarter;
import org.ow2.proactive.scheduler.common.NotificationData;
import org.ow2.proactive.scheduler.common.Scheduler;
import org.ow2.proactive.scheduler.common.SchedulerState;
import org.ow2.proactive.scheduler.common.job.JobId;
import org.ow2.proactive.scheduler.common.job.JobInfo;
import org.ow2.proactive.scheduler.common.job.JobState;
import org.ow2.proactive.scheduler.common.task.TaskInfo;
import org.ow2.proactive.scheduler.common.task.TaskStatus;
import org.ow2.proactive.scheduler.job.InternalTaskFlowJob;
import org.ow2.proactive.scheduler.job.JobIdImpl;
import org.ow2.proactive.scheduler.job.JobInfoImpl;
import org.ow2.proactive.scheduler.task.TaskIdImpl;
import org.ow2.proactive.scheduler.task.TaskInfoImpl;
import org.ow2.proactive.scheduler.task.internal.InternalScriptTask;
import org.ow2.proactive.scheduler.task.internal.InternalTask;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.ow2.proactive.scheduler.common.SchedulerEvent.JOB_RUNNING_TO_FINISHED;
import static org.ow2.proactive.scheduler.common.SchedulerEvent.TASK_RUNNING_TO_FINISHED;

public class IaasPolicyTest {

    private IaasPolicy policy;
    private NodeSource nodeSource;
    private RMCore rmCore;
    private Scheduler scheduler;
    private SchedulerState schedulerState;

    @Before
    public void setUp() throws Exception {
        nodeSource = mock(NodeSource.class);
        rmCore = mock(RMCore.class);
        scheduler = mock(Scheduler.class);
        schedulerState = mock(SchedulerState.class);

        policy = new IaasPolicy();
        policy.setNodeSource(nodeSource);
        policy.setSchedulerForTests(scheduler);
        policy.setSchedulerStateForTests(schedulerState);

        when(nodeSource.getName()).thenReturn("NodeSourceName");
        when(nodeSource.getRMCore()).thenReturn(rmCore);
    }

    @Test
    public void genericInformationAtJobLevel_startsAndStopsInstance() throws Exception {
        JobState job = new InternalTaskFlowJob();
        job.setGenericInformations(createGenericInformation("token"));

        policy.jobSubmittedEvent(job);

        verifyInstanceCreated();

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes("token");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        policy.jobStateUpdatedEvent(new NotificationData<JobInfo>(JOB_RUNNING_TO_FINISHED, createJobInfo(job)));

        verifyInstanceDestroyed();
        verifyTotalNumberOfInstanceCreated(1);
    }

    @Test
    public void genericInformationAtTaskLevel_startsAndStopsInstance() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        InternalTask task = createTask(1, 1);
        task.setGenericInformations(createGenericInformation("token"));
        job.setTasks(Collections.<InternalTask>singletonList(task));

        policy.jobSubmittedEvent(job);
        verifyInstanceCreated();

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes("token");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        policy.jobStateUpdatedEvent(new NotificationData<JobInfo>(JOB_RUNNING_TO_FINISHED, createJobInfo(job)));

        verifyInstanceDestroyed();
        verifyTotalNumberOfInstanceCreated(1);
    }

    @Test
    public void genericInformationAndShutdownAtTaskLevel_startsAndStopsInstanceWhenTaskFinished() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        InternalTask task = createTask(1, 1);
        job.setTasks(Collections.<InternalTask>singletonList(task));

        Map<String, String> genericInformation = createGenericInformation("token");
        genericInformation.put(IaasPolicy.GenericInformation.OPERATION, "DEPLOY_AND_UNDEPLOY");
        task.setGenericInformations(genericInformation);

        policy.jobSubmittedEvent(job);

        verifyInstanceCreated();

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes("token");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        task.setStatus(TaskStatus.FINISHED);
        policy.taskStateUpdatedEvent(new NotificationData<TaskInfo>(TASK_RUNNING_TO_FINISHED, createTaskInfo(task)));

        verifyInstanceDestroyed();
        verifyTotalNumberOfInstanceCreated(1);
    }

    @Test
    public void genericInformationAtJobLevelAndShutdownAtTaskLevel_startsAndStopsInstanceWhenTaskFinished() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        InternalTask task = createTask(1, 1);
        job.setTasks(Collections.<InternalTask>singletonList(task));

        Map<String, String> genericInformation = createGenericInformation("token");
        job.setGenericInformations(genericInformation);
        genericInformation.put(IaasPolicy.GenericInformation.OPERATION, "UNDEPLOY");
        task.setGenericInformations(genericInformation);

        policy.jobSubmittedEvent(job);

        verifyInstanceCreated();

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes("token");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        task.setStatus(TaskStatus.FINISHED);
        policy.taskStateUpdatedEvent(new NotificationData<TaskInfo>(TASK_RUNNING_TO_FINISHED, createTaskInfo(task)));

        verifyInstanceDestroyed();
        verifyTotalNumberOfInstanceCreated(1);
    }

    @Test
    public void genericInformationAtJobLevelAndShutdownAtTaskLevel_Inheritance_startsAndStopsInstanceWhenTaskFinished() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        InternalTask task = createTask(1, 1);
        job.setTasks(Collections.<InternalTask>singletonList(task));

        Map<String, String> genericInformation = createGenericInformation("token");
        job.setGenericInformations(genericInformation);
        // even though token, node source are not set, it should read it from the job generic information
        Map<String, String> taskGenericInformation = new HashMap<String, String>();
        taskGenericInformation.put(IaasPolicy.GenericInformation.OPERATION, "UNDEPLOY");
        task.setGenericInformations(taskGenericInformation);

        policy.jobSubmittedEvent(job);

        verifyInstanceCreated();

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes("token");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        policy.taskStateUpdatedEvent(new NotificationData<TaskInfo>(TASK_RUNNING_TO_FINISHED, createTaskInfo(task)));

        verifyInstanceDestroyed();
        verifyTotalNumberOfInstanceCreated(1);
    }

    @Test
    public void genericInformationAtTaskLevels_startsAndStopsInstanceWhenSecondTaskFinished() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        InternalTask task1 = createTask(1, 1);
        InternalTask task2 = createTask(1, 2);
        job.setTasks(asList(task1, task2));

        Map<String, String> genericInformation = createGenericInformation("token");
        task1.setGenericInformations(genericInformation);
        genericInformation.put(IaasPolicy.GenericInformation.OPERATION, "UNDEPLOY");
        task2.setGenericInformations(genericInformation);

        policy.jobSubmittedEvent(job);

        verifyInstanceCreated();

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes("token");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        task1.setStatus(TaskStatus.FINISHED);
        policy.taskStateUpdatedEvent(new NotificationData<TaskInfo>(TASK_RUNNING_TO_FINISHED, createTaskInfo(task1)));
        verifyInstancesDestroyed(0);

        task2.setStatus(TaskStatus.FINISHED);
        policy.taskStateUpdatedEvent(new NotificationData<TaskInfo>(TASK_RUNNING_TO_FINISHED, createTaskInfo(task2)));

        verifyInstanceDestroyed();
        verifyTotalNumberOfInstanceCreated(1);
    }

    @Test
    public void twoTasksDeployingTwoInstances_StartsTwoInstances() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        InternalTask task1 = createTask(1, 1);
        InternalTask task2 = createTask(1, 2);
        job.setTasks(asList(task1, task2));

        Map<String, String> genericInformation1 = new HashMap<String, String>();
        genericInformation1.put(IaasPolicy.GenericInformation.TOKEN, "token1");
        genericInformation1.put(IaasPolicy.GenericInformation.OPERATION, "DEPLOY");
        task1.setGenericInformations(genericInformation1);

        Map<String, String> genericInformation2 = new HashMap<String, String>();
        genericInformation2.put(IaasPolicy.GenericInformation.TOKEN, "token2");
        genericInformation2.put(IaasPolicy.GenericInformation.OPERATION, "DEPLOY");
        task2.setGenericInformations(genericInformation2);

        Map<String, String> jobGenericInformation = createGenericInformation("");
        jobGenericInformation.remove(IaasPolicy.GenericInformation.OPERATION);
        job.setGenericInformations(jobGenericInformation);

        policy.jobSubmittedEvent(job);

        verifyTotalNumberOfInstanceCreated(2);

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes("token1", "token2");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        task1.setStatus(TaskStatus.FINISHED);
        policy.taskStateUpdatedEvent(new NotificationData<TaskInfo>(TASK_RUNNING_TO_FINISHED, createTaskInfo(task1)));
        verifyInstancesDestroyed(0);

        task2.setStatus(TaskStatus.FINISHED);
        policy.taskStateUpdatedEvent(new NotificationData<TaskInfo>(TASK_RUNNING_TO_FINISHED, createTaskInfo(task2)));
        verifyInstancesDestroyed(0);

        policy.jobStateUpdatedEvent(new NotificationData<JobInfo>(JOB_RUNNING_TO_FINISHED, createJobInfo(job)));
        verifyInstancesDestroyed(2);

        verifyTotalNumberOfInstanceCreated(2);
    }

    @Test
    public void eligibleTasksButAlreadyProvisionned() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        InternalTask task1 = createTask(1, 1);
        InternalTask task2 = createTask(1, 2);
        InternalTask task3 = createTask(1, 3);
        job.setTasks(asList(task1, task2, task3));

        Map<String, String> genericInformation1 = new HashMap<String, String>();
        genericInformation1.put(IaasPolicy.GenericInformation.TOKEN, "token1");
        genericInformation1.put(IaasPolicy.GenericInformation.OPERATION, "DEPLOY");
        task1.setGenericInformations(genericInformation1);

        Map<String, String> genericInformation2 = new HashMap<String, String>();
        genericInformation2.put(IaasPolicy.GenericInformation.TOKEN, "token1");
        task2.addDependence(task1);
        task2.setGenericInformations(genericInformation2);

        // this task will be removed from eligible tasks for two reasons
        // it has unfinished dependencies (task2) and is already provisionned (token1)
        Map<String, String> genericInformation3 = new HashMap<String, String>();
        genericInformation3.put(IaasPolicy.GenericInformation.TOKEN, "token1");
        task3.addDependence(task2);
        task3.setGenericInformations(genericInformation3);

        Map<String, String> jobGenericInformation = createGenericInformation("token1");
        jobGenericInformation.remove(IaasPolicy.GenericInformation.OPERATION);
        job.setGenericInformations(jobGenericInformation);

        policy.jobSubmittedEvent(job);

        verifyTotalNumberOfInstanceCreated(1);

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes("token1");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        task1.setStatus(TaskStatus.FINISHED);
        policy.taskStateUpdatedEvent(new NotificationData<TaskInfo>(TASK_RUNNING_TO_FINISHED, createTaskInfo(task1)));
        verifyInstancesDestroyed(0);
    }

    @Test
    public void severalInstancesRequired() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        Map<String, String> genericInformation = createGenericInformation("token");
        genericInformation.put(IaasPolicy.GenericInformation.INSTANCE_NB, "5");
        job.setGenericInformations(genericInformation);

        policy.jobSubmittedEvent(job);

        verifyInstancesCreated(5);

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes(5, "token");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        policy.jobStateUpdatedEvent(new NotificationData<JobInfo>(JOB_RUNNING_TO_FINISHED, createJobInfo(job)));

        verifyInstancesDestroyed(5);
    }

    @Test
    public void notTheRequiredNodeSource() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        Map<String, String> genericInformation = createGenericInformation("token");
        genericInformation.put(IaasPolicy.GenericInformation.NODE_SOURCE, "unknown");
        job.setGenericInformations(genericInformation);

        policy.jobSubmittedEvent(job);

        verifyTotalNumberOfInstanceCreated(0);

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes("token");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        policy.jobStateUpdatedEvent(new NotificationData<JobInfo>(JOB_RUNNING_TO_FINISHED, createJobInfo(job)));

        verifyInstancesDestroyed(0);
    }

    @Test
    public void instanceCreatedAfterJobExecution() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        job.setGenericInformations(createGenericInformation("token"));

        policy.jobSubmittedEvent(job);

        verifyInstanceCreated();

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> nodeNotYetAlive = createAliveNodes();
        when(nodeSource.getAliveNodes()).thenReturn(nodeNotYetAlive);

        policy.jobStateUpdatedEvent(new NotificationData<JobInfo>(JOB_RUNNING_TO_FINISHED, createJobInfo(job)));

        verifyInstancesDestroyed(0);

        // oh oh the node appears after the job finished
        when(schedulerState.getRunningJobs()).thenReturn(new Vector<JobState>());
        when(schedulerState.getPendingJobs()).thenReturn(new Vector<JobState>());
        LinkedList<Node> aliveNodes = createAliveNodes("token");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);
        RMNodeDescriptor rmNode = mock(RMNodeDescriptor.class);
        when(rmNode.getNodeURL()).thenReturn("rmi://node");

        policy.nodeEvent(new RMNodeEvent(rmNode, RMEventType.NODE_ADDED, NodeState.CONFIGURING, "me"));

        verifyInstanceDestroyed();
    }

    @Test
    public void genericInformationInheritance() throws Exception {
        InternalTaskFlowJob job = new InternalTaskFlowJob();
        InternalTask task = createTask(1, 1);
        job.setTasks(Collections.<InternalTask>singletonList(task));

        Map<String, String> jobGenericInformation = createGenericInformation("");
        jobGenericInformation.remove(IaasPolicy.GenericInformation.OPERATION);
        job.setGenericInformations(jobGenericInformation);
        // even though token, node source are not set, it should read it from the job generic information
        Map<String, String> taskGenericInformation = new HashMap<String, String>();
        taskGenericInformation.put(IaasPolicy.GenericInformation.OPERATION, "DEPLOY");
        taskGenericInformation.put(IaasPolicy.GenericInformation.TOKEN, "token");
        task.setGenericInformations(taskGenericInformation);

        policy.jobSubmittedEvent(job);

        verifyInstanceCreated();

        when(scheduler.getJobState(Matchers.<JobId>any())).thenReturn(job);
        LinkedList<Node> aliveNodes = createAliveNodes("token");
        when(nodeSource.getAliveNodes()).thenReturn(aliveNodes);

        policy.jobStateUpdatedEvent(new NotificationData<JobInfo>(JOB_RUNNING_TO_FINISHED, createJobInfo(job)));

        verifyInstanceDestroyed();
        verifyTotalNumberOfInstanceCreated(1);
    }

    private Map<String, String> createGenericInformation(String token) {
        Map<String, String> genericInformation = new HashMap<String, String>();
        genericInformation.put(IaasPolicy.GenericInformation.NODE_SOURCE, nodeSource.getName());
        genericInformation.put(IaasPolicy.GenericInformation.TOKEN, token);
        genericInformation.put(IaasPolicy.GenericInformation.INSTANCE_NB, "1");
        genericInformation.put(IaasPolicy.GenericInformation.INSTANCE_TYPE, "SMALL");
        genericInformation.put(IaasPolicy.GenericInformation.IMAGE_ID, "imageId");
        genericInformation.put(IaasPolicy.GenericInformation.OPERATION, "DEPLOY");
        return genericInformation;
    }

    private TaskInfo createTaskInfo(InternalTask task) {
        TaskInfoImpl taskInfo = new TaskInfoImpl();
        taskInfo.setTaskId(task.getId());
        taskInfo.setStatus(task.getStatus());
        return taskInfo;
    }

    private void verifyInstanceCreated() {
        verifyInstancesCreated(1);
    }

    private void verifyInstanceDestroyed() {
        verifyInstancesDestroyed(1);
    }

    private void verifyInstancesCreated(int nbOfInstances) {
        verify(nodeSource).acquireNodes(eq(nbOfInstances), anyMap());
    }

    private void verifyTotalNumberOfInstanceCreated(int nbOfCalls) {
        verify(nodeSource, times(nbOfCalls)).acquireNodes(eq(1), anyMap());
    }

    private void verifyInstancesDestroyed(int nb) {
        verify(rmCore, times(nb)).removeNode(anyString(), eq(false));
    }

    private LinkedList<Node> createAliveNodes(String... tokens) throws ProActiveException {
        LinkedList<Node> aliveNodes = new LinkedList<Node>();
        for (String token : tokens) {
            Node node = mock(Node.class);
            when(node.getProperty(RMNodeStarter.NODE_ACCESS_TOKEN)).thenReturn(token);
            NodeInformation nodeInformation = mock(NodeInformation.class);
            when(node.getNodeInformation()).thenReturn(nodeInformation);
            when(nodeInformation.getURL()).thenReturn("rmi://node");
            aliveNodes.add(node);
        }
        return aliveNodes;
    }

    private LinkedList<Node> createAliveNodes(int nb, String token) throws ProActiveException {
        String[] tokens = new String[nb];
        Arrays.fill(tokens, token);
        return createAliveNodes(tokens);
    }

    private JobInfoImpl createJobInfo(JobState jobState) {
        JobInfoImpl jobInfo = new JobInfoImpl();
        jobInfo.setJobId(jobState.getId());
        return jobInfo;
    }

    private InternalTask createTask(int jobId, int taskId) {
        InternalTask task1 = new InternalScriptTask();
        task1.setId(TaskIdImpl.createTaskId(new JobIdImpl(jobId, Integer.toString(jobId)), "task" + taskId, taskId));
        return task1;
    }
}
