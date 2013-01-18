/*
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
package org.ow2.proactive.iaas;

import org.ow2.proactive.resourcemanager.common.event.RMEvent;
import org.ow2.proactive.resourcemanager.common.event.RMNodeEvent;
import org.ow2.proactive.resourcemanager.common.event.RMNodeSourceEvent;
import org.ow2.proactive.resourcemanager.frontend.RMEventListener;
import org.ow2.proactive.resourcemanager.utils.RMNodeStarter;
import org.ow2.proactive.scheduler.common.NotificationData;
import org.ow2.proactive.scheduler.common.Scheduler;
import org.ow2.proactive.scheduler.common.SchedulerEvent;
import org.ow2.proactive.scheduler.common.SchedulerEventListener;
import org.ow2.proactive.scheduler.common.SchedulerState;
import org.ow2.proactive.scheduler.common.job.JobInfo;
import org.ow2.proactive.scheduler.common.job.JobState;
import org.ow2.proactive.scheduler.common.task.CommonAttribute;
import org.ow2.proactive.scheduler.common.task.TaskInfo;
import org.ow2.proactive.scheduler.common.task.TaskState;
import org.ow2.proactive.scheduler.resourcemanager.nodesource.policy.SchedulerAwarePolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.objectweb.proactive.Body;
import org.objectweb.proactive.InitActive;
import org.objectweb.proactive.api.PAActiveObject;
import org.objectweb.proactive.core.ProActiveException;
import org.objectweb.proactive.core.node.Node;
import org.objectweb.proactive.core.util.wrapper.BooleanWrapper;
import org.objectweb.proactive.extensions.annotation.ActiveObject;

import static org.ow2.proactive.iaas.IaasPolicy.GenericInformation.IMAGE_ID;
import static org.ow2.proactive.iaas.IaasPolicy.GenericInformation.INSTANCE_NB;
import static org.ow2.proactive.iaas.IaasPolicy.GenericInformation.INSTANCE_TYPE;
import static org.ow2.proactive.iaas.IaasPolicy.GenericInformation.NODE_SOURCE;
import static org.ow2.proactive.iaas.IaasPolicy.GenericInformation.Operation.isDeployOperation;
import static org.ow2.proactive.iaas.IaasPolicy.GenericInformation.Operation.isUndeployOperation;
import static org.ow2.proactive.iaas.IaasPolicy.GenericInformation.TOKEN;

@ActiveObject
public class IaasPolicy extends SchedulerAwarePolicy implements InitActive, RMEventListener {

    private IaasPolicy thisStub;

    private Set<String> provisionedTokens = new HashSet<String>();

    public void initActivity(Body body) {
        thisStub = (IaasPolicy) PAActiveObject.getStubOnThis();
    }

    @Override
    public BooleanWrapper configure(Object... params) {
        super.configure(params);
        return new BooleanWrapper(true);
    }

    @Override
    public void jobSubmittedEvent(JobState job) {
        acquireNodesIfNeeded(job, job, job.getId().value());
        for (TaskState eligibleTask : getEligibleTasks(job).values()) {
            try {
                acquireNodesIfNeeded(job, eligibleTask, eligibleTask.getId().value());
            } catch (Exception e) {
                logger.error("Failed to retrieve job state", e);
            }
        }
    }

    private void acquireNodesIfNeeded(JobState job, CommonAttribute target, String targetId) {
        if (isRequiredNodeSource(target, job)
                && target.getGenericInformations().containsKey(TOKEN)
                && isDeployOperation(target.getGenericInformations())) {
            acquireNodes(job, target);
        } else {
            logger.debug("This job or task " + targetId + " requires another node source, not provisioning nodes.");
        }
    }

    private Map<String, TaskState> getEligibleTasks(JobState jobState) {
        Map<String, TaskState> eligibleTasks = new HashMap<String, TaskState>();
        Map<String, TaskState> finishedTasks = new HashMap<String, TaskState>();

        // first we consider all tasks waiting for execution are eligible.
        List<TaskState> tasks = jobState.getTasks();
        for (TaskState ts : tasks) {
            switch (ts.getStatus()) {
                case SUBMITTED:
                case PENDING:
                case WAITING_ON_ERROR:
                case WAITING_ON_FAILURE:
                    eligibleTasks.put(ts.getId().value(), ts);
                    break;
                case FINISHED:
                    finishedTasks.put(ts.getId().value(), ts);
                    break;
            }
        }
        // remove from eligible all tasks that have dependencies not contained
        // in finished tasks list
        Iterator<Map.Entry<String, TaskState>> it = eligibleTasks.entrySet().iterator();
        while (it.hasNext()) {
            TaskState taskState = it.next().getValue();
            if (taskState.getDependences() != null) {
                for (TaskState dep : taskState.getDependences()) {
                    if (!finishedTasks.containsKey(dep.getId().value())) {
                        it.remove();
                        break;
                    }

                }
            }
            // Avoid starting an already started instance
            if (taskAlreadyProvisioned(jobState, taskState)) {
                it.remove();
            }
        }
        return eligibleTasks;
    }

    private boolean taskAlreadyProvisioned(JobState jobState, TaskState ts) {
        return provisionedTokens.contains(getGenericInformationFromTaskOrJob(GenericInformation.TOKEN, ts, jobState));
    }

    @Override
    public void taskStateUpdatedEvent(NotificationData<TaskInfo> notification) {
        switch (notification.getEventType()) {
            case TASK_RUNNING_TO_FINISHED:
                try {
                    JobState jobState = scheduler.getJobState(notification.getData().getJobId());
                    TaskState taskState = jobState.getHMTasks().get(notification.getData().getTaskId());
                    removeTokenResourcesUsedByTask(taskState, jobState);

                    for (TaskState eligibleTask : getEligibleTasks(jobState).values()) {
                        JobState job = scheduler.getJobState(eligibleTask.getJobId());
                        acquireNodesIfNeeded(job, eligibleTask, eligibleTask.getId().value());
                    }
                } catch (Exception e) {
                    logger.error("Failed to retrieve job state", e);
                }
                break;
        }
    }

    private void removeTokenResourcesUsedByTask(TaskState task, JobState jobOfTask) {
        if (isRequiredNodeSource(task, jobOfTask)
                && isUndeployOperation(task.getGenericInformations())) {
            // token is mandatory
            logger.debug("Removing nodes used by task " + task.getId());
            removeTokenResources(getGenericInformationFromTaskOrJob(TOKEN, task, jobOfTask));
        }
    }

    private String getGenericInformationFromTaskOrJob(String genericInformationName, CommonAttribute task, JobState jobOfTask) {
        Map<String, String> taskGenericInformation = task.getGenericInformations();
        return taskGenericInformation.containsKey(genericInformationName)
                ? taskGenericInformation.get(genericInformationName)
                : jobOfTask.getGenericInformations().get(genericInformationName);
    }

    private void removeAllTokenResourcesUsedByTask(TaskState taskState, JobState jobOfTask) {
        Map<String, String> genericInformation = taskState.getGenericInformations();
        if (isRequiredNodeSource(taskState, jobOfTask)) {
            logger.debug("Removing nodes used by task " + taskState.getId());
            removeTokenResources(genericInformation.get(TOKEN));
        }
    }

    private boolean isRequiredNodeSource(CommonAttribute task, JobState jobOfTask) {
        return isRequiredNodeSource(task) || isRequiredNodeSource(jobOfTask);
    }

    private boolean isRequiredNodeSource(CommonAttribute job) {
        return nodeSource.getName().equals(job.getGenericInformations().get(NODE_SOURCE));
    }

    private void acquireNodes(JobState job, CommonAttribute target) {
        if (job == null || target == null) {
            return;
        }

        Map<String, String> nodeConfiguration = new HashMap<String, String>();
        nodeConfiguration.put(INSTANCE_TYPE, getGenericInformationFromTaskOrJob(INSTANCE_TYPE, target, job));
        nodeConfiguration.put(IMAGE_ID, getGenericInformationFromTaskOrJob(IMAGE_ID, target, job));
        String jobToken = getGenericInformationFromTaskOrJob(TOKEN, target, job);
        nodeConfiguration.put(TOKEN, jobToken);
        int nbOfInstances = Integer.parseInt(getGenericInformationFromTaskOrJob(INSTANCE_NB, target, job));

        acquireNodes(nbOfInstances, nodeConfiguration);
        provisionedTokens.add(jobToken);
    }

    @Override
    public void jobStateUpdatedEvent(NotificationData<JobInfo> notification) {
        switch (notification.getEventType()) {
            case JOB_PENDING_TO_FINISHED:
            case JOB_RUNNING_TO_FINISHED:
                try {
                    JobState job = scheduler.getJobState(notification.getData().getJobId());
                    removeTokenResourcesUsedByJob(job);
                    for (TaskState task : job.getTasks()) {
                        removeAllTokenResourcesUsedByTask(task, job);
                    }
                } catch (Exception e) {
                    logger.error("Failed to retrieve job state", e);
                }
        }
    }

    private void removeTokenResourcesUsedByJob(JobState jobState) {
        Map<String, String> genericInformation = jobState.getGenericInformations();
        if (isRequiredNodeSource(jobState) && genericInformation.containsKey(TOKEN)) {
            removeTokenResources(genericInformation.get(TOKEN));
        }
    }

    private void removeTokenResources(String token) {
        logger.debug("Removing nodes using token=" + token);
        List<Node> nodesHavingJobToken = findNodesHavingToken(nodeSource.getAliveNodes(), token);

        for (Node node : nodesHavingJobToken) {
            String nodeURL = node.getNodeInformation().getURL();
            logger.debug("Removing node " + nodeURL);
            removeNode(nodeURL, false);
        }
        provisionedTokens.remove(token);
    }

    private List<Node> findNodesHavingToken(LinkedList<Node> nodes, String token) {
        List<Node> nodesWithToken = new ArrayList<Node>();
        for (Node node : nodes) {
            try {
                if (token.equals(readTokenFromNode(node))) {
                    nodesWithToken.add(node);
                }
            } catch (ProActiveException e) {
                logger.warn("Failed to retrieve property", e);
            }
        }
        return nodesWithToken;
    }

    private String readTokenFromNode(Node node) throws ProActiveException {
        return node.getProperty(RMNodeStarter.NODE_ACCESS_TOKEN);
    }

    @Override
    protected SchedulerEvent[] getEventsList() {
        return new SchedulerEvent[]{SchedulerEvent.JOB_RUNNING_TO_FINISHED, SchedulerEvent.JOB_SUBMITTED,
                SchedulerEvent.TASK_RUNNING_TO_FINISHED, SchedulerEvent.JOB_PENDING_TO_FINISHED};
    }

    @Override
    protected SchedulerEventListener getSchedulerListener() {
        return thisStub;
    }

    @Override
    public String getDescription() {
        return "A Iaas policy, aware of the Scheduler and looking for generic information about the kind of instance to start";
    }

    @Override
    public void rmEvent(RMEvent event) {
    }

    @Override
    public void nodeSourceEvent(RMNodeSourceEvent event) {
    }

    @Override
    public void nodeEvent(RMNodeEvent event) {
        switch (event.getEventType()) {
            case NODE_ADDED:
                Node node = filterByNodeUrl(nodeSource.getAliveNodes(), event.getNodeUrl());
                try {
                    String nodeToken = readTokenFromNode(node);
                    if (tokenNotUsedByActiveJobs(nodeToken)) {
                        // node probably registered after job execution
                        logger.warn(String.format("This node %s registered too late, remove it.", node));
                        removeNode(event.getNodeUrl(), false);
                    }
                } catch (ProActiveException e) {
                    logger.warn("Failed to retrieve property", e);
                }
                break;
        }
    }

    private boolean tokenNotUsedByActiveJobs(String token) {
        Vector<JobState> activeJobs = state.getPendingJobs();
        activeJobs.addAll(state.getRunningJobs());

        for (JobState activeJob : activeJobs) {
            if (jobUsesToken(activeJob, token)) {
                return false;
            }
        }
        return true;
    }

    private boolean jobUsesToken(JobState job, String token) {
        if (token.equals(job.getGenericInformations().get(TOKEN))) {
            return true;
        }
        for (TaskState task : job.getTasks()) {
            if (token.equals(task.getGenericInformations().get(TOKEN))) {
                return true;
            }
        }
        return false;
    }

    private Node filterByNodeUrl(LinkedList<Node> nodes, String nodeUrl) {
        for (Node node : nodes) {
            if (nodeUrl.equals(node.getNodeInformation().getURL())) {
                return node;
            }
        }
        throw new IllegalStateException("There should always be one node matching");
    }

    void setSchedulerForTests(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    void setSchedulerStateForTests(SchedulerState state) {
        this.state = state;
    }

    public static class GenericInformation {
        public static final String TOKEN = Scheduler.NODE_ACCESS_TOKEN;
        public static final String NODE_SOURCE = "IAAS_NODE_SOURCE";
        public static final String INSTANCE_NB = "IAAS_INSTANCE_NB";
        public static final String IMAGE_ID = "IAAS_IMAGE_ID";
        public static final String INSTANCE_TYPE = "IAAS_INSTANCE_TYPE";
        public static final String OPERATION = "IAAS_OPERATION";

        public enum Operation {
            DEPLOY, UNDEPLOY, DEPLOY_AND_UNDEPLOY;

            public static boolean isDeployOperation(Map<String, String> genericInformation) {
                return DEPLOY.name().equals(genericInformation.get(OPERATION))
                        || DEPLOY_AND_UNDEPLOY.name().equals(genericInformation.get(OPERATION));
            }

            public static boolean isUndeployOperation(Map<String, String> genericInformation) {
                return UNDEPLOY.name().equals(genericInformation.get(OPERATION))
                        || DEPLOY_AND_UNDEPLOY.name().equals(genericInformation.get(OPERATION));
            }
        }

        public enum InstanceType {
            DEFAULT, SMALL, MEDIUM, LARGE
        }
    }
}
