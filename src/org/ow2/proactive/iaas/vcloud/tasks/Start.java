package org.ow2.proactive.iaas.vcloud.tasks;

import java.io.Serializable;

import org.ow2.proactive.iaas.IaasExecutable;
import org.ow2.proactive.iaas.vcloud.VCloudAPI;
import org.ow2.proactive.scheduler.common.task.TaskResult;


public class Start extends IaasExecutable {

    @Override
    public Serializable execute(TaskResult... results) throws Throwable {
        VCloudAPI api = (VCloudAPI) createApi(args);
        String instanceID = args.get(VCloudAPI.VCloudAPIConstants.InstanceParameters.INSTANCE_ID);
        api.startInstance(api.getIaasInstance(instanceID));

        return null;
    }

}