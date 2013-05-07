/*
 *  
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

import org.ow2.proactive.iaas.monitoring.vmprocesses.VMPLister;
import org.ow2.proactive.iaas.monitoring.vmprocesses.VMProcess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarProxy;
import org.junit.Before;
import org.junit.Test;

@org.junit.Ignore
public class VMProcessTest {

    private static final String INSTANCE = "instance-00000109";
    private static final String UUID = "7de378fc-5f62-4961-a6a7-f3ffc812ad2b";
    private static final String MAC = "fa:16:3e:41:17:68";
    private static final String KVM_COMMAND_LINE_SAMPLE = 
            "/usr/bin/kvm -S -M pc-1.0 -enable-kvm -m 4096 -smp 2,sockets=2,cores=1,threads=1" + 
            " -name " + INSTANCE + " -uuid " + UUID + " -nodefconfig -nodefaults " + 
            "-chardev socket,id=charmonitor,path=/var/lib/libvirt/qemu/instance-00000109.monitor,server,nowait " + 
            "-mon chardev=charmonitor,id=monitor,mode=control -rtc base=utc -no-shutdown " + 
            "-drive file=/store/nova/instances/instance-00000109/disk,if=none,id=drive-virtio-disk0,format=qcow2,cache=none "+ 
            "-device virtio-blk-pci,bus=pci.0,addr=0x4,drive=drive-virtio-disk0,id=virtio-disk0,bootindex=1"+ 
            " -netdev tap,fd=19,id=hostnet0 -device rtl8139,netdev=hostnet0,id=net0,mac="+MAC+",bus=pci.0,addr=0x3 "+
            "-chardev file,id=charserial0,path=/store/nova/instances/instance-00000109/console.log "+
            "-device isa-serial,chardev=charserial0,id=serial0 -chardev pty,id=charserial1 "+
            "-device isa-serial,chardev=charserial1,id=serial1 -usb -device usb-tablet,id=input0 "+
            "-vnc 192.168.1.13:3 -k en-us -vga cirrus -device virtio-balloon-pci,id=balloon0,bus=pci.0,addr=0x5";

    @Before
    public void before() throws Exception {
        startSecondJVM(VMProcessHelperTest.class);
    }

    @Test
    public void testKVMProcess() throws Exception {

        SigarProxy sigar = new Sigar();

        List<VMProcess> vmps = VMPLister.getLocalVMPs(sigar);
        if (vmps.size() != 0) {
            for (VMProcess vmp : vmps) {
                System.out.println("### ### Found VMP...");
                System.out.println("### ### Process information : " + vmp.toString());
            }
        } else {
            System.out.println("### ### No VMP found...");
        }

        Map<String, Object> map = VMPLister.getVMPsAsMap(sigar);
        if (map.keySet().size() != 0) {
            for (String key : map.keySet()) {
                System.out.println("### Key " + key + " : " + map.get(key));
            }
        } else {
            System.out.println("### ### No VMP found...");
        }
        assertTrue(vmps.size() >= 1);

        VMProcess vmp = vmps.get(0);

        assertTrue(vmp.getProperty("uuid").equals(UUID));
        assertTrue(vmp.getProperty("mac").equals(MAC));
        assertTrue(vmp.getProperty("id").equals(INSTANCE));
        
    }

    public static void startSecondJVM(Class<?> clazz) throws Exception {
        String separator = System.getProperty("file.separator");
        String pwd = clazz.getClassLoader().getResource("").getFile();
        String path = System.getProperty("java.home") + separator + "bin" + separator + "java";

        ArrayList<String> array = new ArrayList<String>();
        array.add(path);
        array.add("-cp");
        array.add(pwd);
        array.add(clazz.getCanonicalName());
        array.addAll(Arrays.asList(KVM_COMMAND_LINE_SAMPLE.split(" ")));
        System.out.println("Starting JVM: " + array);

        new ProcessBuilder(array).start();
        
        System.out.println("Done.");
    }

    private void assertTrue(boolean b) throws Exception {
        if (!b) {
            throw new Exception("Test failed.");
        }
    }
}
