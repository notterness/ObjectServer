package com.webutils.distributedshard.client;

/*
** The DistributedShard client is responsible for managing where data is placed between the avaialble Storage Servers.
**
** For placement it uses the following information:
**   Customer
**   Object Prefix
**   Bucket Name
**   Object Name
**   StorageType -
**       Standard
**       Intelligent-Tiering
**       Standard-IA (Infrequent Access)
**       OneZone-IA
**       Archive
**       DeepArchive
**   Object UID - A 64 bit ID that uniquely identified an object being stored
**
** The path to the object is made up the following way:
**     ObjectPrefix/BucketName/ObjectName
*/
public class DistributedShardMain {

    public static void main(string[] args) {
    }
}

