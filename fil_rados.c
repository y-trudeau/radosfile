/* vim: ts=4 sts=4 sw=4 expandtab */
#include <stdio.h>
#include <string.h>
#include <rados/librados.h>
#include <stdlib.h>
#include <jansson.h>

#include "fil_rados.h"

#define	DEBUG 1

const char *METADATA_OBJECT_NAME = "metadata";

json_t *metadata_json = NULL;
json_error_t error_json;

rados_ioctx_t rados_io_context;
rados_t ceph_cluster;


/* Inititialize the rados environment 
   return 0 if successfull, -1 if error */
int fil_rados_init(
	const char* cluster_name, /* name of the cluster */
	const char* user_name, /* auth user for cephx */
	const char* pool_name, /* data pool */
	const char* conf_file /* configuration file */
	) 
{
	int err;
	if ((err = rados_create2(&ceph_cluster, cluster_name, user_name, 0)) < 0) {
		fprintf(stderr, "Error %d: could not create the ceph cluster object\n%s\n",-err,strerror(-err));
		return -1;
	}

	/* Read a Ceph configuration file to configure the cluster handle. */
    if ((err = rados_conf_read_file(ceph_cluster, conf_file)) < 0) {
        fprintf(stderr, "Error %d: cannot read the ceph configuration file\n%s\n", -err, strerror(-err));
	    rados_shutdown(ceph_cluster);
        return -1;
    }

	/* Connecting to the cluster */
	if ((err = rados_connect(ceph_cluster)) < 0) {
        fprintf(stderr, "Error %d: cannot connect to the ceph cluster\n%s\n", -err, strerror(-err));
        rados_shutdown(ceph_cluster);
        return -1;
	}

	/* Opening the IO context */
	if ((err = rados_ioctx_create(ceph_cluster, pool_name, &rados_io_context)) < 0) {
        fprintf(stderr, "Error %d: cannot open rados pool: %s\n%s\n", -err, pool_name, strerror(-err));
        rados_shutdown(ceph_cluster);
        return -1;
	}
	
	/* all good */
	return 0;
}

/* Destroy the rados environment 
   No return value, the only case that could fail is if
   the environment is not setup */
void fil_rados_destroy() {
	rados_ioctx_destroy(rados_io_context);
    rados_shutdown(ceph_cluster);
}

/*  focus on normal io for now 

fil_aio_read() {

}

fil_aio_wait() {

}

fil_aio_write() {

}
*/


/*      
        Close a file and free the sturctures
        return 0 if successfull, -1 if error 
*/
int fil_close(FILErados_t* fp) {
	
	if (fp) {
		if (fp->metadata.name) {
            /* Decrement number of reference */
            _fil_decrement_n_ref(fp->metadata.name,fp->metadata.type);

            json_t *file;
            file = _fil_get_json_metadata(fp->metadata.name,fp->metadata.type);
            
            if (_fil_get_n_ref(file) == 0 && _fil_is_deleted(file) == 1) {
                /* this was a deleted file kept open, now it is time
                 * to really delete it
                 */
                 fil_delete_file(fp->metadata.name,fp->metadata.type);
            } 
            
			free(fp->metadata.name);
			fp->metadata.name = NULL;
		} 
		
		free(fp);
		fp = NULL;
	} 
	return 0;
}


/* 	
	Create and open a new file
	return the file handle if successfull or NULL if an error occurred 
*/
FILErados_t* fil_open_create(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type, /* file object type, seen enum def */
	size_t block_size /* block size to use in rados */
	) 
	{  

	if (!block_size) {
		fprintf(stderr, "Error: uninitialized block size value, can't be zero\n");
		return NULL;
	}

	if (!filepath) {
		fprintf(stderr, "Error: uninitialized file path can't be null\n");
		return NULL;
	}

	/* checking if the path exists in the metadata */
	if (_fil_find_in_metadata(filepath,type) == -1) {
		/* Adding the path to the metadata */
		if (_fil_add_file_metadata(filepath,type,0,block_size) < 0) {
			return NULL;
		}
	}
	
	return fil_open(filepath, type);
}


/*      
        Delete a file
        return 0 if successfull, -1 if error 
*/
int fil_delete_file(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type /* file object type, seen enum def */
)
{
	/* checking if the path exists in the metadata */
	int index = _fil_find_in_metadata(filepath,type);
	if (index < 0) {
		fprintf(stderr, "Error: file %s does not exist\n", filepath);
		return -1;
	}
	
	/* get the json file element */
	json_t *file;
	file = json_array_get(metadata_json,index);
	if (!file) {
		fprintf(stderr, "Error extracting the file object from the metadata\n");
		return -1;
	}

	if (type == OS_FILE_TYPE_FILE) {
		
        if (_fil_get_n_ref(file) == 0) {            
        	if (_fil_delete_rados_objects(filepath, _fil_get_block_size(file))) {
                /* if there's an error, it is already reported */
                json_decref(file);
                return -1;
            }

            /* All good to remove the metadata and objects */
            if (_fil_rm_file_metadata(filepath,type)) {
                /* if there's an error, it is already reported */
                json_decref(file);
                return -1;
            }

        } else {
            /* mark as deleted in metadata */
        	if (_fil_set_deleted(filepath, type)) {
                /* if there's an error, it is already reported */
                json_decref(file);
                return -1;
            }
        }
    }
    
    json_decref(file);
    
    /* TODO handle of other types, especially  OS_FILE_TYPE_DIR */
    
	return 0;
}

/* Flush all data and wait until done */
void fil_flush() {

	rados_aio_flush(rados_io_context);

}


/* 	
	Open an existing file
	return the file handle if successfull or NULL if an error occurred 
*/
FILErados_t* fil_open( 
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type /* file object type, seen enum def */
)
{  
	/* checking if the path exists in the metadata */
	if (_fil_find_in_metadata(filepath,type) < 0) {
		fprintf(stderr, "Error: file %s does not exist\n", filepath);
		return NULL;
	}

	FILErados_t* fp;
	fp = malloc(sizeof(FILErados_t));
	if (!fp) {
		fprintf(stderr, "Error: unable to allocate memory fo file %s handle\n", filepath);
		return NULL;
	}	

	if (_fil_get_file_metadata(filepath,type,fp) < 0) {
		fprintf(stderr, "Error: unable to initialize the file handle for file %s\n", filepath);
		free(fp);	
		return NULL;
	}
    
    
    if (_fil_increment_n_ref(filepath,type) < 0) {
		fprintf(stderr, "Error: couldn't increment n_ref for file %s\n", filepath);
		free(fp);	
		return NULL;        
    }

	/* Initialize the position to 0 */
	fp->position=0;

	return fp;
}

/*      
        Read from a file in rados 
        return the number of bytes read if successfull, -1 if error 
*/
ssize_t fil_read(
	FILErados_t*    fp,	/* handle to a file */
	void*		buf,	/* buffer where to read */
	size_t		len,	/* number of bytes to read */
	size_t		offset  /* offset from where to start reading */
) {
	size_t bytes_read = 0;
	size_t total_bytes_read = 0;
	size_t block_offset;
	char*	obj_name;  

    /* TODO, implementing aio here could be very efficient on multi block reads */

	if (!fp) {
		fprintf(stderr, "Error: uninitialized file handle\n");
		return -1;
	}

	if (!fp->metadata.block_size) {
		fprintf(stderr, "Error: uninitialized block size value, can't be zero\n");
		return -1;
	}

	if (!fp->metadata.name) {
		fprintf(stderr, "Error: uninitialized file name, can't be null\n");
		return -1;
	}

	block_offset = offset/fp->metadata.block_size;  /* this will cast to int */
	block_offset = block_offset*fp->metadata.block_size; /* now point to the beginning of a block */

	/* read first object */	
	obj_name = (char *) (intptr_t) asprintf("%s_%zu",fp->metadata.name,block_offset);
    if ((fp->metadata.block_size - (offset - block_offset)) > len) {
        /* all fit in the first block */
        if ((bytes_read = rados_read(rados_io_context,obj_name,buf,len,
                offset - block_offset)) < 0) {
            fprintf(stderr, "Error: Could not read %s at offset %zu\n",obj_name,offset);
            free(obj_name);
            return -1;
        }
    } else {
        if ((bytes_read = rados_read(rados_io_context,obj_name,buf,
                fp->metadata.block_size - offset,offset - block_offset)) < 0) {
            fprintf(stderr, "Error: Could not read %s at offset %zu\n",obj_name,offset);
            free(obj_name);
            return -1;
        }        
    }
	total_bytes_read += bytes_read;
	free(obj_name);
    
    /* now, the offset part is done, just need to care about the 
     * number of bytes to read 
     */
    while (total_bytes_read < len) {
        /* The next block */
        block_offset += fp->metadata.block_size;
        obj_name = (char *) (intptr_t) asprintf("%s_%zu",fp->metadata.name,block_offset);
        
        if ((len - total_bytes_read) > fp->metadata.block_size) {
            /* reading the full block */
            if ((bytes_read = rados_read(rados_io_context,obj_name,buf+total_bytes_read,
                    fp->metadata.block_size,0)) < 0) {
                fprintf(stderr, "Error: Could not read %s\n",obj_name);
                free(obj_name);
                return -1;
            }
        } else {
            /* reading the reminder */
            if ((bytes_read = rados_read(rados_io_context,obj_name,buf+total_bytes_read,
                    len - total_bytes_read,0)) < 0) {
                fprintf(stderr, "Error: Could not read %s\n",obj_name);
                free(obj_name);
                return -1;
            }   
        }

        total_bytes_read += bytes_read;
        free(obj_name);
        
        /* Maybe we are done */
        if (bytes_read == 0) {
            /* no more */
            break;
        }
    }
    
    return total_bytes_read;
    
}

/*  
 * Write to a file in rados 
 * 
 * Returns the number of bytes written if successfull, -1 if error 
*/
int fil_write(	
    FILErados_t*    fp,	/* handle to a file */
	void*		buf,	/* buffer where to get data to write */
	size_t		len,    /* number of bytes to write */
	size_t		offset  /* offset from where to start reading */
    ) 
{
	size_t bytes_written = 0;
	size_t total_bytes_written = 0;
	size_t block_offset;
	char*	obj_name;  

    /* TODO, implementing aio here could be very efficient on multi block writes */

	if (!fp) {
		fprintf(stderr, "Error: uninitialized file handle\n");
		return -1;
	}

	if (!fp->metadata.block_size) {
		fprintf(stderr, "Error: uninitialized block size value, can't be zero\n");
		return -1;
	}

	if (!fp->metadata.name) {
		fprintf(stderr, "Error: uninitialized file name, can't be null\n");
		return -1;
	}

	block_offset = offset/fp->metadata.block_size;  /* this will cast to int */
	block_offset = block_offset*fp->metadata.block_size; /* now point to the beginning of the firs
                                                            block to write to */

	/* write to the first object */	
	obj_name = (char *) (intptr_t) asprintf("%s_%zu",fp->metadata.name,block_offset);
    if ((fp->metadata.block_size - (offset - block_offset)) > len) {
        /* all fit in the first block */
        if ((bytes_written = rados_write(rados_io_context,obj_name,buf,
                len,offset - block_offset)) < 0) {
            fprintf(stderr, "Error: Could not write %s at offset %zu\n",obj_name,offset);
            free(obj_name);
            return -1;
        }
    } else {
        if ((bytes_written = rados_write(rados_io_context,obj_name,
                buf,fp->metadata.block_size-offset,offset)) < 0) {
            fprintf(stderr, "Error: Could not write %s at offset %zu\n",obj_name,offset);
            free(obj_name);
            return -1;
        }        
    }
	total_bytes_written += bytes_written;
	free(obj_name);

    /* now, the offset part is done, just need to care about the 
     * number of bytes to write 
     */
    while (total_bytes_written < len) {
        /* The next block */
        block_offset += fp->metadata.block_size;
        obj_name = (char *) (intptr_t) asprintf("%s_%zu",fp->metadata.name,block_offset);
        
        if ((len - total_bytes_written) > fp->metadata.block_size) {
            /* writing a full block */
            if ((bytes_written = rados_write(rados_io_context,obj_name,buf+total_bytes_written,
                    fp->metadata.block_size,0)) < 0) {
                fprintf(stderr, "Error: Could not write %s\n",obj_name);
                free(obj_name);
                return -1;
            }
        } else {
            /* writing the reminder */
            if ((bytes_written = rados_read(rados_io_context,obj_name,buf+total_bytes_written,
                    len - total_bytes_written,0)) < 0) {
                fprintf(stderr, "Error: Could not write %s\n",obj_name);
                free(obj_name);
                return -1;
            }   
        }

        total_bytes_written += bytes_written;
        free(obj_name);
        
        /* Maybe we are done */
        if (bytes_written == 0) {
            /* no more */
            break;
        }
    }
    
    return total_bytes_written;

}

/* not needed for now 
fil_update_atime() {

} */

/* not needed for now 
fil_update_mtime() {

} */









/*      
        (pseudoPrivate) Return the block size of the file json 
        return the block_size if successfull (> 0), -1 if error 
*/
int _fil_get_block_size(
	json_t *jfile   /* json file element */
	) 
{
    
    if (json_is_object(jfile)) { 
        /* get the block_size */
        unsigned int block_size;
        json_t *j_block_size;
        j_block_size = (json_t *) (intptr_t) json_object_get(jfile,"block_size");
        if(!json_is_integer(j_block_size)) {
            fprintf(stderr, "error: block_size element returned is not an integer\n");
            json_decref(j_block_size);
            return -1;
        } else {
            block_size = (unsigned int) json_integer_value(j_block_size);
        }
        json_decref(j_block_size);
        
        return block_size;
    } else {
        return -1;
    }
}

/*      
        (pseudoPrivate) Return the number of reference of the file json 
        Returns: number of reference if successfull (> 0), -1 if error 
*/
int _fil_get_n_ref(
	json_t *jfile   /* json file element */
	) 
{
    
    if (json_is_object(jfile)) { 
        /* get the n_ref */
        unsigned int n_ref;
        json_t *j_n_ref;
        j_n_ref = (json_t *) (intptr_t) json_object_get(jfile,"n_ref");
        if(!json_is_integer(j_n_ref)) {
            fprintf(stderr, "error: n_ref element returned is not an integer\n");
            json_decref(j_n_ref);
            return -1;
        } else {
            n_ref = (unsigned int) json_integer_value(j_n_ref);
        }
        json_decref(j_n_ref);
        
        return n_ref;
    } else {
        return -1;
    }
}


/*      
        (pseudoPrivate) Increment the number of reference of a file
        return 0 if successfull, -1 if error 
*/
int _fil_set_n_ref(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type,
    int delta  /* typically 1 or -1 */
	) 
{
	/* TODO:  may need a mutex when used with multiple threads */
	int index = _fil_find_in_metadata(filepath,type);
	json_t *file = json_array_get(metadata_json,index);
	if (!file) {
		fprintf(stderr, "Error extracting the file object from the metadata\n");
		return -1;
	}

    unsigned int n_ref;
    json_t *j_n_ref;
    j_n_ref = (json_t *) (intptr_t) json_object_get(file,"n_ref");
    if(!json_is_number(j_n_ref)) {
        fprintf(stderr, "error for entry  %d: n_ref is not a number\n", index + 1);
        json_decref(file);
        json_decref(j_n_ref);
        return -1;
    }

    n_ref = (unsigned int) json_integer_value(j_n_ref);
    json_decref(j_n_ref);
    
    n_ref += delta;
    
    /* sanity check */
    if (n_ref < 0) n_ref = 0;
    
	if (json_object_set(file,"n_ref",json_integer(n_ref)) < 0) {
		fprintf(stderr, "Error updating the number of reference of file object\n");
		return -1;
	}
	
	if (json_array_set(metadata_json,index,file) < 0) {
		fprintf(stderr, "Error updating the file object in the metadata\n");
		return -1;
	}

	json_decref(file);
    
    return 0;

    /* 
     * We don't need to update metadata on disk, n_ref is useless there
     * since it is set to 0 on load
     */
}

/*      
        (pseudoPrivate) Increment the number of reference of a file
        return 0 if successfull, -1 if error 
*/
int _fil_increment_n_ref(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type
	) 
{
    return _fil_set_n_ref(filepath,type,1);
}

/*      
        (pseudoPrivate) Decrement the number of reference of a file
        return 0 if successfull, -1 if error 
*/
int _fil_decrement_n_ref(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type
	) 
{
    return _fil_set_n_ref(filepath,type,-1);
}


/*
 *     (pseudoPrivate) Return if the file is delete or not in the metadata 
        Returns: 0 if not deleted, 1 if deleted and -1 on error
*/
unsigned int _fil_is_deleted(
	json_t *file   /* json file element */
	) 
{
    
    if (json_is_object(file)) { 
        /* get the n_ref */
        unsigned int deleted;
        json_t *j_deleted;
        j_deleted = (json_t *) (intptr_t) json_object_get(file,"deleted");
        if(!json_is_integer(j_deleted)) {
            fprintf(stderr, "error: deleted element returned is not an integer\n");
            json_decref(j_deleted);
            return -1;
        } else {
            deleted = (unsigned int) json_integer_value(j_deleted);
        }
        json_decref(j_deleted);
        
        return deleted;
    } else {
        return -1;
    }
}

/*      
        (pseudoPrivate) Update the size of a file after a write
        return 0 if successfull, -1 if error 
*/
int _fil_update_size(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type, /* file object type, seen enum def */
	size_t new_size /* new file size */
	) 
{
	/* TODO:  may need a mutex when used with multiple threads */
	int index = _fil_find_in_metadata(filepath,type);
	json_t *file = (json_t *) (intptr_t) json_array_get(metadata_json,index);
	if (!file) {
		fprintf(stderr, "Error extracting the file object from the metadata\n");
		return -1;
	}

	if (json_object_set(file,"size",json_integer(new_size)) < 0) {
		fprintf(stderr, "Error updating the size of file object\n");
		return -1;
	}
	
	if (json_array_set(metadata_json,index,file) < 0) {
		fprintf(stderr, "Error updating the file object in the metadata\n");
		return -1;
	}

	json_decref(file);

	if (_fil_update_metadata_json() < 0) {
		return -1;
	} else {
		return 0;
	}
	
}

/*      
        (pseudoPrivate) Set the deleted flag of a file in the metadata
        return 0 if successfull, -1 if error 
*/
int _fil_set_deleted(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type
	) 
{
	/* TODO:  may need a mutex when used with multiple threads */
	int index = _fil_find_in_metadata(filepath,type);
	json_t *file = (json_t *) (intptr_t) json_array_get(metadata_json,index);
	if (!file) {
		fprintf(stderr, "Error extracting the file object from the metadata\n");
		return -1;
	}

	if (json_object_set(file,"deleted",json_integer(1)) < 0) {
		fprintf(stderr, "Error setting deleted in the file object\n");
		return -1;
	}
	
	if (json_array_set(metadata_json,index,file) < 0) {
		fprintf(stderr, "Error updating the file object in the metadata\n");
		return -1;
	}

	json_decref(file);

	if (_fil_update_metadata_json() < 0) {
		return -1;
	} else {
		return 0;
	}
	
}

/*      
        Delete a file in rados
        return 0 if successfull, -1 if error 
*/
int _fil_delete_rados_objects(
    const char* filepath,  /* path of the file */
    const unsigned int block_size 
    ) 
{
    
    /* TODO, verify it is ok to remore */
        
    size_t pos = 0;
    char* obj_name;
    while (1) {
        obj_name = (char *) (intptr_t) asprintf("%s_%zu",filepath,pos);
        if (!rados_remove(rados_io_context,obj_name)) {
            if (DEBUG) {
                fprintf(stderr, "DEBUG: rados_remove object %s\n", obj_name);
            }
        } else {
            if (DEBUG) {
                fprintf(stderr, "DEBUG: done removing ojects from rados"); 
            }
            free(obj_name);
            break;
        }
        /* increasing the position by the block_size */
        pos += block_size;
        free(obj_name);
    }
    return 0;
}

/* 	
	(pseudoPrivate) Load the metadata in memory
	return 0 successful, -1 if error 
*/
int _fil_load_metadata_json() 
{
	/* Is it already loaded */
	if (metadata_json) {
		return 0;
	}

	/* test if ioctx is set */
	struct rados_pool_stat_t pstat;
	if (rados_ioctx_pool_stat(rados_io_context,&pstat) < 0) {
		fprintf(stderr, "Error accessing Ceph, invalid IO context\n");
		return -1;
	}

	/* read the metadata object stat */
	uint64_t	metadata_size;
	time_t		metadata_mtime;
	if (rados_stat(rados_io_context,METADATA_OBJECT_NAME,&metadata_size,&metadata_mtime)) {
		fprintf(stderr, "Error stating Metadata\n");
		return -1;
	}

	/* Allocate the buffer for the metadata */	
	char		*bufmetadata;
	bufmetadata = (char *) (intptr_t) malloc(metadata_size);
	if (!bufmetadata) {
		fprintf(stderr, "Error allocating memory for Metadata buffer\n");
		return -1;
	}

	/* Read the metadata object */
	if (rados_read(rados_io_context,METADATA_OBJECT_NAME,bufmetadata,metadata_size,0) < 0) {
		fprintf(stderr, "Error reading metadata from rados\n");
		free(bufmetadata);
		return -1;
	}

	/* parse in json */
	metadata_json = json_loads(bufmetadata, 0, &error_json);
	if(!metadata_json) {
        fprintf(stderr, "Error loading json on line %d: %s\n", error_json.line, error_json.text);
		free(bufmetadata);
        return -1;
	}
	
	/* Free the text buffer */
	free(bufmetadata);

    /* Some files may have been deleted but kept open, we need to check and
     * cleanup in case the application crashed
     */
    int i;
    for(i = json_array_size(metadata_json) - 1;i >= 0;i--) {
        /* going backward because we may have to remove elements */
        char *file_path;
        unsigned int block_size;
        unsigned int deleted;
            
        json_t *jfile, *jdeleted, *jpath, *jblock_size;
        jfile = json_array_get(metadata_json,i);
        if (!jfile) {
            fprintf(stderr, "Error retrieving metadata for file %d: %s\n", i, error_json.text);
            return -1;
        }
        
        jdeleted = (json_t *) (intptr_t) json_object_get(jfile,"deleted");
		if(!json_is_integer(jdeleted)) {
            fprintf(stderr, "error: deleted element returned is not an integer\n");
            json_decref(jfile);
            json_decref(jdeleted);
            return -1;
        } else {
			deleted = (unsigned int) json_integer_value(jdeleted);
            json_decref(jdeleted);
            
            if (deleted == 1) {
                /* The file was deleted, we need to get the filepath
                 * and block_size
                 */
                 
                jpath = (json_t *) (intptr_t) json_object_get(jfile,"path");
                if(!json_is_string(jpath)) {
                    fprintf(stderr, "error: path element returned is not a string\n");
                    json_decref(jfile);
                    json_decref(jpath);
                    return -1;
                } else {
                    file_path = strdup(json_string_value(jpath));
                    json_decref(jpath);
                }
                
                jblock_size = (json_t *) (intptr_t) json_object_get(jfile,"block_size");
                if(!json_is_integer(jblock_size)) {
                    fprintf(stderr, "error: block_size element returned is not an integer\n");
                    json_decref(jblock_size);
                    json_decref(jfile);
                    return -1;
                } else {
                    block_size = (unsigned int) json_integer_value(jblock_size);
                    json_decref(jblock_size);
                }
                
                _fil_delete_rados_objects(file_path,block_size);
                
                json_decref(jfile);
                
                /* Now, removing the entry from the metadata object */
                if (json_array_remove(metadata_json,i)) {
                    fprintf(stderr, "error: Unable to remove the file element for the metadata\n");
                    return -1;
                }
            } 
                
		}
		json_decref(jfile);
        
    }
	return 0;
	
}


/* 	
	(pseudoPrivate) Load the metadata in memory
	return 0 if successful, -1 if error
*/
int _fil_update_metadata_json() {
	/* Is it already loaded */
	if (!metadata_json) {
		/* no... so shouldn't save */
		return -1;
	}

	/* test if ioctx is set */
	struct rados_pool_stat_t pstat;
	if (rados_ioctx_pool_stat(rados_io_context,&pstat) < 0) {
		fprintf(stderr, "Error accessing Ceph, invalid IO context\n");
		return -1;
	}

	char *buffer;
	buffer = json_dumps(metadata_json,JSON_COMPACT);

	if (!buffer) {
		fprintf(stderr, "Error dumping the internal metadata json\n");
		return -1;
	}

	if (rados_write_full(rados_io_context, METADATA_OBJECT_NAME, buffer, strlen(buffer)) < 0) {
		fprintf(stderr, "Error writing the metadata object to ceph\n");
		free(buffer);
		return -1;
	}

	free(buffer);
	return 0;		
}

/* 	
	(pseudoPrivate) find a file in the metadata
	returns -3 on error
    returns -2 if the file exists but is deleted (but still opened)
	returns -1 if the path doesn't not exist 
	returns the array index in metadata if the file exists
*/
int _fil_find_in_metadata(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type /* file object type, seen enum def */
	) 
{
	/* is the metadata json loaded? */
	if (!metadata_json) {
		_fil_load_metadata_json();	
	}
	
	/* TODO:  may need a mutex when used with multiple threads */
	if(!json_is_array(metadata_json)) {
        fprintf(stderr, "error: metadata_json is not an array\n");
		json_decref(metadata_json);
        return -3;
	}

    int i;
    json_t *jdata, *jpath, *jtype, *jdeleted;
	for(i = 0; i < json_array_size(metadata_json); i++) {
		
		jdata = (json_t *) (intptr_t) json_array_get(metadata_json, i);
        if(!json_is_object(jdata)) {
            fprintf(stderr, "error, file entry %d is not a json object\n", i + 1);
            json_decref(metadata_json);
            return -3;
		}

		jpath = (json_t *) (intptr_t) json_object_get(jdata, "path");
		if(!json_is_string(jpath)) {
			fprintf(stderr, "error for entry %d, fpath is not a string\n", i + 1);
			json_decref(jdata);
			json_decref(metadata_json);
            return -3;
        }
		
		jtype = (json_t *) (intptr_t) json_object_get(jdata,"type");
		if(!json_is_number(jtype)) {
            fprintf(stderr, "error for entry  %d: ftype is not a number\n", i + 1);
			json_decref(jdata);
			json_decref(jpath);
            json_decref(metadata_json);
            return -3;
		}
        
		jdeleted = (json_t *) (intptr_t) json_object_get(jdata,"deleted");
		if(!json_is_number(jtype)) {
            fprintf(stderr, "error for entry  %d: jdeleted is not an integer\n", i + 1);
			json_decref(jdata);
			json_decref(jpath);
            json_decref(metadata_json);
            return -3;
		}        

        if (json_integer_value(jdeleted) == 1) {
            json_decref(jpath);
			json_decref(jdata);
			json_decref(jtype);
			json_decref(jdeleted);
            return -2;
        }

		if ((strcmp(filepath,json_string_value(jpath)) == 0 ) && type == (os_file_type_t) json_integer_value(jtype)) {
			json_decref(jpath);
			json_decref(jdata);
			json_decref(jtype);
			json_decref(jdeleted);
			return i;
		}

		json_decref(jdata);
	}
	json_decref(jpath);
	json_decref(jdata);
	json_decref(jtype);
	json_decref(jdeleted);
	
	return -1;
}
	

/* 	
	(pseudoPrivate) Add a file to the metadata
	return 0 if successfull -1 if error
*/
int _fil_add_file_metadata(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type, /* file object type, seen enum def */
	size_t size,  /* Size of the file */
	size_t blockSize /* blockSize */
	) 
{
	/* is the metadata json loaded? */
	if (!metadata_json) {
		_fil_load_metadata_json();	
	}
	
	if (_fil_find_in_metadata(filepath,type) >= 0) {
        fprintf(stderr, "error: file %s can't be added, it already exists in metadata\n",filepath);
		return -1;
	}

	if(!json_is_array(metadata_json)) {
        fprintf(stderr, "error: metadata_json is not an array\n");
		json_decref(metadata_json);
		return -1;
	}

	json_t *jsonObj = (json_t *) (intptr_t) json_object();
	if (json_object_set_new(jsonObj,"type",json_integer(type)) < 0) {
        fprintf(stderr, "error: unable to add type to new json object\n");
        json_decref(jsonObj);
		return -1;
	}
	if (json_object_set_new(jsonObj,"deleted",json_integer(0)) < 0) {
        fprintf(stderr, "error: unable to add deleted to new json object\n");
        json_decref(jsonObj);
		return -1;
	}
    if (json_object_set_new(jsonObj,"nref",json_integer(0)) < 0) {
        fprintf(stderr, "error: unable to add nref to new json object\n");
        json_decref(jsonObj);
		return -1;
	}
	if (json_object_set_new(jsonObj,"size",json_integer(size)) < 0) {
        fprintf(stderr, "error: unable to add size to new json object\n");
        json_decref(jsonObj);
		return -1;
	}
	if (json_object_set_new(jsonObj,"block_size",json_integer(blockSize)) < 0) {
        fprintf(stderr, "error: unable to add block_size to new json object\n");
        json_decref(jsonObj);
		return -1;
	}
	if (json_object_set_new(jsonObj,"path",json_string(filepath)) < 0) {
        fprintf(stderr, "error: unable to add path to new json object\n");
        json_decref(jsonObj);
		return -1;
	}	
	if (json_array_append(metadata_json,jsonObj) < 0) {
        fprintf(stderr, "error: unable to add the new json object to the metadata json array\n");
        json_decref(jsonObj);
		return -1;
	}

	/* we're done with jsonObj */
	json_decref(jsonObj);

	/* update in ceph */
	if (_fil_update_metadata_json() < 0) {
		return -1;
	}

	return 0;
}

int _fil_rm_file_metadata(
        char* filepath,   /* file path like sbtest/sbtest.ibd */
        os_file_type_t type /* file object type, seen enum def */
        )
{
	
	/* TODO:  may need a mutex when used with multiple threads */
	int index = _fil_find_in_metadata(filepath,type);

	if(index < 0) {
		/* error should be reported to stderr in _fil_find_in_metadata */
        return -1;
	} else {
		if (json_array_remove(metadata_json,index) < 0) {
			fprintf(stderr, "error: unable to remove the file %s from the metadata json object\n",filepath);
            return -1;	
		}
	}

	/* update in ceph */
	if (_fil_update_metadata_json() < 0) {
		return -1;
	}

	return 0;
}

/* 	
 * (pseudoPrivate) Return the json metadata entry of a file.  The caller
 * is responsible to call json_decref on the object.
 * return json_t* or NULL if not successfull -1 if error
*/
json_t* _fil_get_json_metadata(
        char* filepath,   /* file path like sbtest/sbtest.ibd */
        os_file_type_t type /* file object type, seen enum def */
        )
{
	/* TODO:  may need a mutex when used with multiple threads */
	int index = _fil_find_in_metadata(filepath,type);
    json_t *file;
    
	if(index < 0) {
		/* error should be reported to stderr in _fil_find_in_metadata */
        return NULL;
	} else {
		file = json_array_get(metadata_json,index);
		if (!file) {
			fprintf(stderr, "Error extracting the file object from the metadata\n");
			return NULL;
		}
    }
    
    return file;
}
/* 	
	(pseudoPrivate) Allocate and initialize
	the file descriptor, fp is allocated in fil_open or fil_create 
	return 0 if successfull -1 if error
*/
int _fil_get_file_metadata(
        char* filepath,   /* file path like sbtest/sbtest.ibd */
        os_file_type_t type, /* file object type, seen enum def */
        FILErados_t*	fp  /* rados file FILE struct */
        )
{
	/* TODO:  may need a mutex when used with multiple threads */
	int index = _fil_find_in_metadata(filepath,type);

    json_t *jfile, *jpath, *jsize, *jblock_size, *jtype, *jdeleted, *jn_ref;
	if(index < 0) {
		/* error should be reported to stderr in _fil_find_in_metadata */
        return -1;
	} else {
		jfile = json_array_get(metadata_json,index);
		if (!jfile) {
			fprintf(stderr, "Error extracting the file object from the metadata\n");
			return -1;
		}
		
		jpath = (json_t *) (intptr_t) json_object_get(jfile,"path");
		if(!json_is_string(jpath)) {
            fprintf(stderr, "error: path element returned is not a string\n");
            json_decref(jfile);
            return -1;
        } else {
			fp->metadata.name = strdup(json_string_value(jpath));
		}
		json_decref(jpath);

		jtype = (json_t *) (intptr_t) json_object_get(jfile,"type");
		if(!json_is_integer(jtype)) {
            fprintf(stderr, "error: type element returned is not an integer\n");
            json_decref(jfile);
            return -1;
        } else {
			fp->metadata.type = (int) json_integer_value(jtype);
		}
		json_decref(jtype);

		jdeleted = (json_t *) (intptr_t) json_object_get(jfile,"deleted");
		if(!json_is_integer(jdeleted)) {
            fprintf(stderr, "error: deleted element returned is not an integer\n");
            json_decref(jfile);
            json_decref(jdeleted);
            return -1;
        } else {
			fp->metadata.deleted = (int) json_integer_value(jdeleted);
		}
		json_decref(jdeleted);

		jblock_size = (json_t *) (intptr_t) json_object_get(jfile,"jblock_size");
		if(!json_is_integer(jblock_size)) {
            fprintf(stderr, "error: block_size element returned is not an integer\n");
            json_decref(jfile);
            json_decref(jblock_size);
            return -1;
        } else {
			fp->metadata.block_size = (unsigned int) json_integer_value(jblock_size);
		}
		json_decref(jblock_size);

		jsize = (json_t *) (intptr_t) json_object_get(jfile,"size");
		if(!json_is_integer(jsize)) {
            fprintf(stderr, "error: size element returned is not an integer\n");
            json_decref(jfile);
       		json_decref(jsize);
            return -1;
        } else {
			fp->metadata.size = (size_t) json_integer_value(jsize);
		}
		json_decref(jsize);

		jn_ref = (json_t *) (intptr_t) json_object_get(jfile,"nref");
		if(!json_is_integer(jn_ref)) {
            fprintf(stderr, "error: nref element returned is not an integer\n");
			json_decref(jn_ref);
            json_decref(jfile);
            return -1;
        } else {
			fp->metadata.n_ref = (size_t) json_integer_value(jn_ref);
		}
		json_decref(jn_ref);
	}	
	json_decref(jfile);

	return 0;
}

