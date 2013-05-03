package org.ow2.proactive.iaas.vcloud.tasks;

import java.io.Serializable;

import org.ow2.proactive.iaas.IaasExecutable;
import org.ow2.proactive.iaas.vcloud.VCloudAPI;
import org.ow2.proactive.scheduler.common.task.TaskResult;


public class Undeploy extends IaasExecutable {

    @Override
    public Serializable execute(TaskResult... results) throws Throwable {
        String vappId = results[0].toString();

        VCloudAPI api = (VCloudAPI) createApi(args);
        args.put(VCloudAPI.VCloudAPIConstants.InstanceParameters.INSTANCE_ID, results[0].toString());
        api.undeployInstance(api.getIaasInstance(vappId));

        return results[0];
    }

}