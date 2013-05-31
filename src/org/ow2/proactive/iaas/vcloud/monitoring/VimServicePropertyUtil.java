package org.ow2.proactive.iaas.vcloud.monitoring;

import static org.ow2.proactive.iaas.vcloud.monitoring.VimServiceConstants.PROP_HOST_CPU_CORES;
import static org.ow2.proactive.iaas.vcloud.monitoring.VimServiceConstants.PROP_HOST_CPU_FREQUENCY;
import static org.ow2.proactive.iaas.vcloud.monitoring.VimServiceConstants.PROP_HOST_CPU_USAGE;
import static org.ow2.proactive.iaas.vcloud.monitoring.VimServiceConstants.PROP_HOST_MEMORY_TOTAL;
import static org.ow2.proactive.iaas.vcloud.monitoring.VimServiceConstants.PROP_HOST_MEMORY_USAGE;
import static org.ow2.proactive.iaas.vcloud.monitoring.VimServiceConstants.PROP_VM_CPU_CORES;
import static org.ow2.proactive.iaas.vcloud.monitoring.VimServiceConstants.PROP_VM_CPU_FREQUENCY;
import static org.ow2.proactive.iaas.vcloud.monitoring.VimServiceConstants.PROP_VM_CPU_USAGE;
import static org.ow2.proactive.iaas.vcloud.monitoring.VimServiceConstants.PROP_VM_MEMEORY_TOTAL;
import static org.ow2.proactive.iaas.vcloud.monitoring.VimServiceConstants.PROP_VM_MEMORY_USAGE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VimServicePropertyUtil {

    public static class VM {
        public static void standardize(Map<String, String> propertyMap) {
            replaceKeyIfPresent(VimServiceConstants.PROP_VM_PARENT, "host",
                    propertyMap);
            setCupUsageProperties(PROP_VM_CPU_CORES, PROP_VM_CPU_FREQUENCY,
                    PROP_VM_CPU_USAGE, propertyMap);
            setVmMemoryUsageProperties(propertyMap);
            setVmStorageUsageProperties(propertyMap);
            replaceKeyIfPresent(VimServiceConstants.PROP_VM_NETWORK,
                    "network.count", propertyMap);
            standardizeCommonProperties(propertyMap);
        }
    }

    public static class HOST {
        public static void standardize(Map<String, String> propertyMap) {
            setCupUsageProperties(PROP_HOST_CPU_CORES, PROP_HOST_CPU_FREQUENCY,
                    PROP_HOST_CPU_USAGE, propertyMap);
            setHostMemoryUsageProperties(propertyMap);
            setHostStorageUsageProperties(propertyMap);
            replaceKeyIfPresent(VimServiceConstants.PROP_HOST_NETWORK_COUNT,
                    "network.count", propertyMap);
            replaceKeyIfPresent(VimServiceConstants.PROP_HOST_SITE, "site",
                    propertyMap);
            standardizeCommonProperties(propertyMap);
        }
    }

    private static void standardizeCommonProperties(
            Map<String, String> propertyMap) {
        replaceKeyIfPresent(VimServiceConstants.PROP_NET_RX_RATE, "network.rx",
                propertyMap);
        replaceKeyIfPresent(VimServiceConstants.PROP_NET_TX_RATE, "network.tx",
                propertyMap);
        replaceKeyIfPresent(VimServiceConstants.PROP_NET_USAGE,
                "network.speed", propertyMap);
        replaceStatus(VimServiceConstants.PROP_STATE, "status", propertyMap);
    }
    
    private static void setCupUsageProperties(final String numOfCoresKey,
            final String cpuFrequencyKey, final String cpuUsageKey,
            Map<String, String> propertyMap) {
        int numOfCores = Integer.valueOf(propertyMap.remove(numOfCoresKey));
        propertyMap.put("cpu.cores", String.valueOf(numOfCores));
        
        float fghz = 0, usage = 0;
        if (propertyMap.containsKey(cpuFrequencyKey)) {
            long fMHz = Long.valueOf(propertyMap.remove(cpuFrequencyKey));
            fghz = ((float) fMHz) / 1000;
            if (propertyMap.containsKey(cpuUsageKey)) {
                long uMHz = Long.valueOf(propertyMap.remove(cpuUsageKey));
                usage = (((float) uMHz) / (numOfCores * fMHz));
            }
        }
        propertyMap.put("cpu.frequency", String.valueOf(fghz));
        propertyMap.put("cpu.usage", String.valueOf(usage));
    }

    private static void setHostMemoryUsageProperties(
            Map<String, String> propertyMap) {
        long capacity = 0, used = 0, free =0;
        if (propertyMap.containsKey(PROP_HOST_MEMORY_TOTAL)) {
            capacity = Long.valueOf(propertyMap.remove(PROP_HOST_MEMORY_TOTAL));
        }
        if (propertyMap.containsKey(PROP_HOST_MEMORY_USAGE)) {
            used = mbytes2bytes(propertyMap.remove(PROP_HOST_MEMORY_USAGE));
        }
        if (capacity > used) {
            free = capacity - used;
        }
        propertyMap.put("memory.total", String.valueOf(capacity));
        propertyMap.put("memory.used", String.valueOf(used));
        propertyMap.put("memory.free", String.valueOf(free));
    }
    
    private static void setHostStorageUsageProperties(
            Map<String, String> propertyMap) {
        long capacity = 0, free = 0, used = 0;
        List<String> toRemove = new ArrayList<String>();
        for (String key : propertyMap.keySet()) {
            if (key.startsWith("host.datastore.")) {
                toRemove.add(key);
                if (key.endsWith(".total")) {
                    capacity = Long.parseLong(propertyMap.get(key));
                } else if (key.endsWith(".free")) {
                    free = Long.parseLong(propertyMap.get(key));
                }
                if (capacity != 0 && free != 0) {
                    break;
                }
            }
        }
        if (capacity > free) {
        used = capacity - free;
        }
        propertyMap.put("storage.total", String.valueOf(capacity));
        propertyMap.put("storage.free", String.valueOf(free));
        propertyMap.put("storage.used", String.valueOf(used));
        
        removeAll(toRemove, propertyMap);
    }
    
    private static void setVmMemoryUsageProperties(
            Map<String, String> propertyMap) {
        long totalInBytes = 0, usageInBytes = 0, freeInBytes = 0;
        if (propertyMap.containsKey(PROP_VM_MEMEORY_TOTAL)) {
            totalInBytes = mbytes2bytes(propertyMap
                    .remove(PROP_VM_MEMEORY_TOTAL));
        }
        if (propertyMap.containsKey(PROP_VM_MEMORY_USAGE)) {
            usageInBytes = mbytes2bytes(propertyMap
                    .remove(PROP_VM_MEMORY_USAGE));
        }
        if (totalInBytes > usageInBytes) {
            freeInBytes = totalInBytes - usageInBytes;
        }
        propertyMap.put("memory.total", String.valueOf(totalInBytes));
        propertyMap.put("memory.used", String.valueOf(usageInBytes));
        propertyMap.put("memory.free", String.valueOf(freeInBytes));
    }
    
    private static void setVmStorageUsageProperties(
            Map<String, String> propertyMap) {
        long total = 0;
        long free = 0;
        long used = 0;
        List<String> toRemove = new ArrayList<String>();
        for (String key : propertyMap.keySet()) {
            if (key.startsWith("vm.disk.")) {
                toRemove.add(key);
                long value = Long.valueOf(propertyMap.get(key));
                if (key.endsWith(".total")) {
                    total += value;
                } else if (key.endsWith(".free")) {
                    free += value;
                } else if (key.endsWith(".used")) {
                    used += value;
                } else {
                    // FIXME: consider throwing an exception
                }
            }
        }
        
        propertyMap.put("storage.total", String.valueOf(total));
        propertyMap.put("storage.free", String.valueOf(free));
        propertyMap.put("storage.used", String.valueOf(used));
        
        removeAll(toRemove, propertyMap);
    }

    private static void replaceStatus(String oldKey, String newKey,
            Map<String, String> propertyMap) {
        String status = propertyMap.remove(oldKey);
        if (status != null) {
            status = ("POWERED_ON".equalsIgnoreCase(status)) ? "up" : "down";
            propertyMap.put(newKey, status);
        }
    }

    private static void replaceKeyIfPresent(String oldKey, String newKey,
            Map<String, String> propertyMap) {
        if (propertyMap.containsKey(oldKey)) {
            propertyMap.put(newKey, propertyMap.remove(oldKey));
        }
    }
    
    private static long mbytes2bytes(String value) {
        return Long.parseLong(value) * 1024 * 1024;        
    }
    
    private static void removeAll(List<String> keys, Map<String,String> properties) {
        for (String key : keys) {
            properties.remove(key);
        }
    }
    
    // non-instantiable
    private VimServicePropertyUtil() {
    }

}
