import requests, json, sys
import requests.packages.urllib3

requests.packages.urllib3.disable_warnings()

class XtremObject:
   def __init__(self,object_data, xtremio_connection):
       """ Simple class to make working with XtremIO return objects easier """
       self.data = object_data
       self.name = object_data["name"]
       self.href = object_data["href"]
       self.parent_connection = xtremio_connection

       # Object name and type comes off the end of the href URL
       self.object_id = object_data["href"].split("/")[-1] 
       self.object_type = object_data["href"].split("/")[-2]
         
   def details(self):
       return self.parent_connection._get_details(self)

   def __repr__(self):
       return "<XtremObject: %s>" % self.object_type

class XtremIO:
    def __init__(self,ip,user,pwd):
        """ Constructor, store user, pass, and IP address """
        self.user = user
        self.pwd = pwd
        self.ip = ip
        self.active_cluster_id = None
        self.api_endpoint = 'https://%s/api/json/v2/types/' % self.ip

    def __repr__(self):
        """  repr:  <XtremIO IP: <IP ADDR> > """
        return "<XtremIO IP: %s>" % self.ip

    def _get_objects(self, object_type, **kwargs):
        """ Gathers requested objects of object_type from array 
            Returning a list of XtremObjects """

        params = dict()
        if kwargs:
            params = kwargs

        if self.active_cluster_id:
            params["cluster_id"] = self.active_cluster_id

        try:  
            response = requests.get(self.api_endpoint + object_type, 
                                    auth=(self.user,self.pwd), 
                                    params=params, verify=False)

            devices = json.loads(response.text)

        except requests.exceptions.RequestException as e:
            print "Error:",e
            return 1

        return_objects = []
        for i in devices.keys():
            if i == u"links":
                continue 
            for j in devices[i]:
                return_objects.append(XtremObject(j,self))
        return return_objects

    def _get_details(self, device_object, **kwargs):
        """ Gathers details of a specific object, returning a 
            dict as returned from the array """
        params = dict()
        if kwargs:
            params = kwargs

        if self.active_cluster_id:
            params["cluster_id"] = self.active_cluster_id

        try:  
            response = requests.get(device_object.href,
                                    auth=(self.user,self.pwd), 
                                    params=params, verify=False)
            info = json.loads(response.text)
            return info["content"]
        except requests.exceptions.RequestException as e:
            print "Error:",e
            return 1
       
    def set_cluster(self,cluster):
        """ Sets the active cluster, this is really a convenience for
            XMS systems managing multiple clusters """
        if isinstance(cluster,XtremObject):
            self.active_cluster_id = cluster.object_id
            return True
        
        # If we pass in a cluster name or raw id 
        for cluster in self.get_clusters():
            if cluster == cluster.name:
                self.active_cluster_id = cluster.object_id
                return True
            if cluster == cluster.object_id:
                self.active_cluster_id = cluster.object_id
                return True
       
        return None

    def get_xbricks(self):
        """ Provides a list of all managed xbricks """
        return self._get_objects("bricks")
    
    def get_xbrick_detail(self,brick):
        """ Provides the details on a specific brick """
        return self._get_details(brick)

    def get_clusters(self):
        """ Gets a list of all clusters on the XMC """
        return self._get_objects("clusters")

    def get_cluster_detail(self,cluster):
        """ Provides the details on a specific cluster """
        return self._get_details(cluster)

    def get_ssds(self):
        """ Provides a list of all SSDs in the cluster """
        return self._get_objects("ssds")

    def get_ssd_details(self,ssd):
        """ Provides details on a specific SSD """
        return self._get_details(ssd)
                 
    def get_slots(self):
        """ Provides a list of all slots in the cluster """
        return self._get_objects("slots") 

    def get_slot_details(self,slot):
        return self._get_details(slot)

    def get_ibswitches(self):
        """ Provides a list of all switches in the cluster """
        return self._get_objects("infiniband-switches")

    def get_ibswitch_details(self,ibswitch):
        return self._get_details(ibswitch)

    def get_bbus(self):
        """ Provides a list of all BBUs in the cluster """
        return self._get_objects("bbus")

    def get_bbu_details(self,bbu):
        return self._get_details(bbu)

    def get_daes(self):
        """ Provides a list of all DAEs in the cluster """
        return self._get_objects("daes")
    
    def get_dae_details(self,dae):
        return self._get_details(dae)
   
    def get_daecontrollers(self):
        """ Provides a list of all DAE Controller cards in the cluster """
        return self._get_objects("daecontrollers")

    def get_daecontroller_details(self,dae):
        return self._get_details(daecontroller)

    def get_daepsus(self):
        """ Provides a list of all DAE PSUs in the cluster """
        return self._get_objects("daepsus")

    def get_daepsu_details(self,dae):
        return self._get_details(daepsu)
    
    def get_localdisks(self):
        """ Provides a list of local disks in the SCs in the cluster """
        return self._get_objects("local-disks")

    def get_localdisk_details(self,localdisk):
        return self._get_details(localdisk)
    
    def get_storagecontrollers(self):
        """ Provides a list of all SCs in the cluster """
        return self._get_objects("storage-controllers")

    def get_storagecontroller_details(self,sc):
        return self._get_details(sc)

    def get_storagecontrollerpsus(self):
        return self._get_objects("storage-controller-psus")

    def get_storagecontrollerpsus_details(self,scpsus):
        return self._get_details(scpsus)

if __name__ == "__main__":

    ip = "10.5.39.118"
    array = XtremIO(ip,"admin","Xtrem10")

    clusters = array.get_clusters()
    array.set_cluster(clusters[0])
    bricks = array.get_xbricks()
    a = array.get_storagecontrollerpsus()[1]
    b = a.details()
    print a
    print b

