package org.apache.storm.topology;

import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;

import java.util.Enumeration;
import java.util.HashMap;

public class StormPolicy extends Policy{

    private HashMap<String, Permissions> permMap = new HashMap<>();
    private HashMap<CodeSource, String> csMap = new HashMap<>();

    @Override
    public PermissionCollection getPermissions(ProtectionDomain domain){
	//idk how to get name from protectiondomain. that seems too sketch and overloading too much
	//HashMap<CodeSource, Permissions>??
	//idk if the CodeSources are different enough but it might work?
       
        CodeSource cs = domain.getCodeSource();
	
	Permissions permissions = permMap.get(csMap.get(cs));
	for(int i = 0; i < 25; i++)
	    System.out.println("");
	System.out.println(permissions);
	return permissions;
    }

    public void addPerm(String name, Permissions perm){
	for(int i = 0; i < 25; i++)
            System.out.println("");
        System.out.println("Went into addPerm");
	permMap.put(name, perm);
    }
    
    public void addBoltPerm(String name, String spoutName){
	if(permMap.containsKey(name)){
	    Permissions newPerm = new Permissions();
	    Permissions permBolt = permMap.get(name);
	    Enumeration<Permission> enumBolt = permBolt.elements();
	    while(enumBolt.hasMoreElements()){
		Permission boltPerm = enumBolt.nextElement();
		Permissions permSpout = permMap.get(spoutName);
		Enumeration<Permission> enumSpout = permSpout.elements();
		while(enumSpout.hasMoreElements()){
		    Permission spoutPerm = enumSpout.nextElement();
		    if(boltPerm.equals(spoutPerm)){
			newPerm.add(boltPerm);
		    }
		}
	    }
	    permMap.replace(name, newPerm);
	}
	else{
	    permMap.put(name, permMap.get(spoutName));
	}
    }
    
    public void addCS(String name, CodeSource cs){
	csMap.put(cs, name);
    }
    
}