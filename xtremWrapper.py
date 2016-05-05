import requests, json, sys
import requests.packages.urllib3

requests.packages.urllib3.disable_warnings()

class XtremObject(object):
   def __init__(self,object_data, xtremio_connection):
       """Parent class to make working with XtremIO returned objects easier """
       self.data = object_data
       self.name = object_data["name"]
       self.href = object_data["href"]
       self.parent_connection = xtremio_connection

       # Object name and type comes off the end of the href URL
       self.object_id = object_data["href"].split("/")[-1] 
       self.object_type = object_data["href"].split("/")[-2]

       # We track the sys-id of the system the object is from, since we will
       # regularly need it
       self.initial_object_details = self.get_details()
       self.sys_id = self.initial_object_details["sys-id"]
     
   def get_details(self, **kwargs):
       return self.parent_connection._get_details(self, **kwargs)

   def __repr__(self):
       return "<XtremObject: %s>" % self.object_type

class XtremVolume(XtremObject):
    """ XtremVolume subclass of XtremObject, provides functions for 
    properties and functions specifically around a volume """

    @classmethod
    def is_class_for(cls, object_type):
        return object_type == "volumes"

    def __init__(self, object_data, xtremio_connection):
        XtremObject.__init__(self, object_data, xtremio_connection)
 
    @property
    def snapshots(self):
        volume_details = self.get_details()
  
        if "dest-snap-list" in volume_details:
            snap_list = []
            for i in volume_details["dest-snap-list"]:
                snap_index = i[2]
                snap_list += self.parent_connection.get_snapshots(index=snap_index)
            return snap_list
      
        return None

class XtremSSD(XtremObject):
    @classmethod
    def is_class_for(cls, object_type):
        return object_type == "ssds"

    def __init__(self, object_data, xtremio_connection):
        XtremObject.__init__(self, object_data, xtremio_connection)
        self.ssd_id = self.initial_object_details["ssd-id"]

    def __repr__(self):
        return "<XtremSSD: Cluster id: %s Drive id: %s>" % (self.sys_id, self.object_id)

class XtremDAE(XtremObject):
    @classmethod
    def is_class_for(cls, object_type):
        return object_type == "daes"

    def __init__(self, object_data, xtremio_connection):
        XtremObject.__init__(self, object_data, xtremio_connection)
        self.jbod_id = self.initial_object_details["jbod-id"]

    def __repr__(self):
        return "<XtremDAE: Cluster id: %s DAE id: %s>" % (self.sys_id, self.jbod_id)

    def _get_objects(self, object_type, **kwargs):
        """ Gathers the associated objects, with the necessary filters
        in the REST request """

        return self.parent_connection._get_objects(object_type,
                                            sys_id=self.sys_id,
                                            jbod_id=self.jbod_id,
                                            **kwargs) 

    @property
    def bricks(self, **kwargs):
        return self._get_objects("bricks")

    @property 
    def ssds(self, **kwargs):
        return self._get_objects("ssds")

    @property 
    def slots(self, **kwargs):
        return self._get_objects("slots") 

    @property 
    def daes(self, **kwargs):
        return self._get_objects("daes")
    
    @property 
    def daecontrollers(self, **kwargs):
        return self._get_objects("dae-controllers")

    @property 
    def daepsus(self, **kwargs):
        return self._get_objects("dae-psus")


class XtremeDAEController(XtremObject):
    @classmethod
    def is_class_for(cls, object_type):
        return object_type == "dae-controllers"

    def __init__(self, object_data, xtremio_connection):
        XtremObject.__init__(self, object_data, xtremio_connection)
        self.controller_id = self.initial_object_details["jbod-controller-id"]

    def __repr__(self):
        return "<XtremDAEController: Cluster id: %s DAEcon id: %s>" % (self.sys_id, self.controller_id)

class XtremeDAEPSU(XtremObject):
    @classmethod
    def is_class_for(cls, object_type):
        return object_type == "dae-psus"

    def __init__(self, object_data, xtremio_connection):
        XtremObject.__init__(self, object_data, xtremio_connection)
        psu_id = self.initial_object_details["jbod-psu-id"]

    def __repr__(self):
        return "<XtremDAEPSU: Cluster id: %s DAEPSU id: %s>" % (self.sys_id, self.psu_id)

class XtremeSlot(XtremObject):
    @classmethod
    def is_class_for(cls, object_type):
        return object_type == "slots"

    def __init__(self, object_data, xtremio_connection):
        XtremObject.__init__(self, object_data, xtremio_connection)
        self.slot_num = self.initial_object_details["slot-num"]

    def __repr__(self):
        return "<XtremSlot: Cluster id: %s Slot Num: %s>" % (self.sys_id, self.slot_num)

    
class XtremBrick(XtremObject):
    @classmethod
    def is_class_for(cls, object_type):
        return object_type == "bricks"

    def __init__(self, object_data, xtremio_connection):
        XtremObject.__init__(self, object_data, xtremio_connection)
        self.brick_id = self.initial_object_details["brick-id"]

    def __repr__(self):
        return "<XtremBrick: Cluster id: %s Brick id: %s>" % (self.sys_id, self.brick_id)

    def _get_objects(self, object_type, **kwargs):
        """ Gathers the associated objects, with the necessary filters
        in the REST request """

        return self.parent_connection._get_objects(object_type,
                                            sys_id=self.sys_id,
                                            brick_id=self.brick_id,
                                            **kwargs) 

    @property 
    def ssds(self, **kwargs):
        return self._get_objects("ssds")

    @property 
    def slots(self, **kwargs):
        return self._get_objects("slots") 

    @property 
    def daes(self, **kwargs):
        return self._get_objects("daes")
    
    @property 
    def daecontrollers(self, **kwargs):
        return self._get_objects("dae-controllers")

    @property 
    def daepsus(self, **kwargs):
        return self._get_objects("dae-psus")

    @property 
    def localdisks(self, **kwargs):
        return self._get_objects("local-disks")

    @property 
    def storagecontrollers(self, **kwargs):
        return self._get_objects("storage-controllers")

    @property 
    def storagecontrollerpsus(self, **kwargs):
        return self._get_objects("storage-controller-psus")

class XtremCluster(XtremObject):

    @classmethod
    def is_class_for(cls, object_type):
        return object_type == "clusters"

    def __init__(self, object_data, xtremio_connection):
        XtremObject.__init__(self, object_data, xtremio_connection)
    
    def __repr__(self):
        return "<XtremCluster: ID=%s>" % self.object_id

    def _get_objects(self, object_type, **kwargs):
        """ Gathers the associated objects, with the necessary filters
        in the REST request """
        return self.parent_connection._get_objects(object_type,
                                            sys_id=self.sys_id,
                                            **kwargs) 

    @property 
    def bricks(self, **kwargs):
        return self._get_objects("bricks")
 
    @property 
    def ssds(self, **kwargs):
        return self._get_objects("ssds")

    @property 
    def slots(self, **kwargs):
        return self._get_objects("slots") 

    @property 
    def ibswitches(self, **kwargs):
        return self._get_objects("infiniband-switches")

    @property 
    def bbus(self, **kwargs):
        return self._get_objects("bbus")

    @property 
    def daes(self, **kwargs):
        return self._get_objects("daes")
    
    @property 
    def daecontrollers(self, **kwargs):
        return self._get_objects("dae-controllers")

    @property 
    def daepsus(self, **kwargs):
        return self._get_objects("dae-psus")

    @property 
    def localdisks(self, **kwargs):
        return self._get_objects("local-disks")

    @property 
    def storagecontrollers(self, **kwargs):
        return self._get_objects("storage-controllers")

    @property 
    def storagecontrollerpsus(self, **kwargs):
        return self._get_objects("storage-controller-psus")

    @property 
    def volumes(self, **kwargs):
        return self._get_objects("volumes")

    @property 
    def initiatorgroups(self, **kwargs):
        return self._get_objects("initiator-groups")

    @property 
    def initiators(self, **kwargs):
        return self._get_objects("initiators")

    @property 
    def snapshots(self, **kwargs):
        return self._get_objects("snapshots")

    @property 
    def snapshotsets(self, **kwargs):
        return self._get_objects("snapshot-sets")

    @property 
    def consistencygroups(self, **kwargs):
        return self._get_objects("consistency-groups")

    @property 
    def lunmaps(self, **kwargs):
        return self._get_objects("lun-maps")
          
    @property 
    def tags(self, **kwargs):
        return self._get_objects("tags")

    @property 
    def targets(self, **kwargs):
        return self._get_objects("targets")

    @property 
    def targetgroups(self, **kwargs):
        return self._get_objects("target-groups")

def XtremObjFactory(object_type, object_data, parent_connection):
    """ Picks the right object class for us based on the object_type """
    for cls in XtremObject.__subclasses__():
        if cls.is_class_for(object_type):
            return cls(object_data, parent_connection)
 
class XtremIO:
    def __init__(self,ip,user,pwd):
        """ Constructor, store user, pass, and IP address """
        self.user = user
        self.pwd = pwd
        self.ip = ip
        self.api_endpoint = 'https://%s/api/json/v2/types/' % self.ip

        self.clusters = self._get_objects("clusters")
        self.xms = self._get_objects("xms")

    def __repr__(self):
        """  repr:  <XtremIO IP: <IP ADDR> > """
        return "<XtremIO IP: %s>" % self.ip

    def _get_objects(self, object_type, **kwargs):
        """ Gathers requested objects of object_type from array 
            Returning a list of XtremObjects """
        params = dict()
        if kwargs:
            for key, val in kwargs.items():
                if '_' in key:
                    new_key = key.replace("_","-")  
                    params[new_key] = val
                else:
                    params[key] = val
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
                return_objects.append(XtremObjFactory(object_type,j,self))

        return return_objects

    def _get_details(self, device_object, **kwargs):
        """ Gathers details of a specific object, returning a 
            dict as returned from the array """
        params = dict()
        if kwargs:
            for key, val in kwargs.items():
                if '_' in key:
                    new_key = key.replace("_","-")  
                    params[new_key] = val
                else:
                    params[key] = val

        try:  
            response = requests.get(device_object.href,
                                    auth=(self.user,self.pwd), 
                                    params=params, verify=False)
            info = json.loads(response.text)
            return info["content"]
        except requests.exceptions.RequestException as e:
            print "Error:",e
            return 1
