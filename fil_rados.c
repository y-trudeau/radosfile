#include <stdio.h>
#include <string.h>
#include <rados/librados.h>
#include <stdlib.h>
#include <jansson.h>

#include "fil_rados.h"

#define	DEBUG 1

json_t *metadata_json = NULL;
json_error_t error_json;

rados_ioctx_t rados_io_context;

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
	
	/* wondering how that function could fail... */

	if (fp) {
		if (fp->metadata.name) {
			free(fp->metadata.name);
			fp->metadata.name = NULL;
		} 
		
		free(fp);
		fp = NULL;
	} 
	return 0
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
		return -1;
	}

	if (!filepath) {
		fprintf(stderr, "Error: uninitialized file path can't be null\n");
		return -1;
	}

	/* checking if the path exists in the metadata */
	if (_fil_find_in_metadata(filepath,type) == -1) {
		/* Adding the path to the metadata */
		if (_fil_add_metadata(filepath,type,block_size) < 0) {
			return NULL;
		}
	}
	
	return fil_open(filepath, fp);
}


/*      
        Delete a file in rados
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
		/* get the block_size */
		unsigned int block_size;
		json_t *j_block_size;
		j_block_size = json_get_object(file,"block_size");
		if(!json_is_integer(j_block_size))
    		{
	        	fprintf(stderr, "error: block_size element returned is not an integer\n");
	       		json_decref(file);
       			return -1;
	    	} else {
			block_size = (unsigned int) json_integer_value(j_block_size);
		}
		json_decref(j_block_size);

		/* remove the rados objects */
		size_t pos = 0;
		char* obj_name;
		while (1) {
			sprintf(obj_name,"%s_%zu",filepath,pos);
			if (!rados_remove(rados_io_context,obj_name)) {
				if (DEBUG) {
					fprintf(stderr, "DEBUG: rados_remove object %s\n", obj_name);
				}
			} else {
				if (DEBUG) {
					fprintf(stderr, "DEBUG: done removing ojects from rados"); 
				}
				break;		
			}
			/* increasing the position by the block_size */
			pos += block_size;
		}
	}

	json_decref(j_block_size);
	if (_fil_rm_file_metadata(filepath,type)) {
       		/* if there's an error, it is already reported */
		return -1;
	}
	
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

	/* Initialize the position to 0 */
	fp->position=0;

	return
}

/*      
        Read from a file in rados 
        return the number of bytes read if successfull, -1 if error 
*/
ssize_t fil_read(
	FILErados_t*    fp,	/* handle to a file */
	void*		buf,	/* buffer where to read */
	size_t		n,	/* number of bytes to read */
	size_t		offset  /* offset from where to start reading */
) {
	size_t bytes_read = 0;
	size_t block_offset;
	char*	radosbuf;

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

	radosbuf = malloc(fp->metadata.block_size);
	
	
	

}

fil_write() {

}

/* not needed for now 
fil_update_atime() {

} */

/* not needed for now 
fil_update_mtime() {

} */

/*      
        (pseudoPrivate) Update the size of a file after a write
        return 0 if successfull, -1 if error 
*/
int _fil_update_size(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type, /* file object type, seen enum def */
	new_size size_t  /* new file size */
	) 
{
	/* TODO:  may need a mutex when used with multiple threads */
	int index = _fil_find_in_metadata(filepath,type);
	json_t *file = json_array_get(metadata_json,index);
	if (!file) {
		fprintf(stderr, "Error extracting the file object from the metadata\n");
		return -1;
	}

	if (json_object_set(file,"size",new_size) < 0) {
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
	rados_pool_stat_t pstat;
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
	bufmetadata = (char *) malloc(metadata_size);
	if (!bufmetadata) {
		fprintf(stderr, "Error allocating memory for Metadata buffer\n");
		return -1;
	}

	/* Read the metadata object */
	if (rados_read(rados_io_context,bufmetadata,metadata_size,0) < 0) {
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
	rados_pool_stat_t pstat;
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
	return -2 on error
	return -1 if the path doesn't not exist 
	return the array index in metadata if the file exists
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
    		return -2;
	}

	for(i = 0; i < json_array_size(metadata_json); i++) {
		json_t *fdata, *fpath, *ftype;

		fdata = json_array_get(metadata_json, i);
	    	if(!json_is_object(fdata)) {
        		fprintf(stderr, "error, file entry %d is not a json object\n", i + 1);
	        	json_decref(metadata_json);
        		return -2;
		}

		fpath = json_object_get(data, "path");
		if(!json_is_string(fpath))
    		{
			fprintf(stderr, "error for entry %d, fpath is not a string\n", i + 1);
			json_decref(metadata_json);
        		return -2;
    		}
		
		ftype = json_object_get(data,"type");
		if(!json_is_number(ftype)) {
		        fprintf(stderr, "error for entry  %d: ftype is not a number\n", i + 1);
		        json_decref(metadata_json);
		        return -2;
		}

		if ((strcmp(filepath,json_string_value(fpath)) == 0 ) && type == (os_file_type_t) json_integer_value(ftype)) {
			json_decref(fpath);
			json_decref(fdata);
			json_decref(ftype);
			return i;
		}

		json_decref(fdata);
	}
	json_decref(fpath);
	json_decref(fdata);
	json_decref(ftype);
	
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
    		fprintf(stderr, "error: file %s can't be added, it already exists in metadata\n");
		return -1;
	}

	if(!json_is_array(metadata_json)) {
    		fprintf(stderr, "error: metadata_json is not an array\n");
		json_decref(metadata_json);
		return -1;
	}

	json_t *jsonObj = json_object();
	if (json_object_set_new(jsonObj,"type",json_integer(type)) < 0) {
    		fprintf(stderr, "error: unable to add type to new json object\n");
		return -1;
	}
	if (json_object_set_new(jsonObj,"size",json_integer(size)) < 0) {
    		fprintf(stderr, "error: unable to add size to new json object\n");
		return -1;
	}
	if (json_object_set_new(jsonObj,"block_size",json_integer(blockSize)) < 0) {
    		fprintf(stderr, "error: unable to add block_size to new json object\n");
		return -1;
	}
	if (json_object_set_new(jsonObj,"path",json_string(filepath)) < 0) {
    		fprintf(stderr, "error: unable to add path to new json object\n");
		return -1;
	}	
	if (json_array_append(metadata_json,jsonObj) < 0) {
    		fprintf(stderr, "error: unable to add the new json object to the metadata json array\n");
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

	if(index < 0) {
		/* error should be reported to stderr in _fil_find_in_metadata */
                return -1;
	} else {
		json_t *file, *path, *size, *block_size, *type;
		file = json_array_get(metadata_json,index);
		if (!file) {
			fprintf(stderr, "Error extracting the file object from the metadata\n");
			return -1;
		}
		
		path = json_get_object(file,"path");
		if(!json_is_string(path))
    		{
		        fprintf(stderr, "error: file element returned is not a string\n");
        		json_decref(file);
        		return -1;
    		} else {
			fp->metadata.name = strdup(json_string_value(path));
		}
		json_decref(path);

		type = json_get_object(file,"type");
		if(!json_is_integer(type))
    		{
		        fprintf(stderr, "error: type element returned is not an integer\n");
        		json_decref(file);
        		return -1;
    		} else {
			fp->metadata.type = (int) json_integer_value(type);
		}
		json_decref(type);

		block_size = json_get_object(file,"block_size");
		if(!json_is_integer(block_size))
    		{
		        fprintf(stderr, "error: block_size element returned is not an integer\n");
        		json_decref(file);
        		return -1;
    		} else {
			fp->metadata.block_size = (unsigned int) json_integer_value(block_size);
		}
		json_decref(block_size);

		size = json_get_object(file,"size");
		if(!json_is_integer(size))
    		{
		        fprintf(stderr, "error: size element returned is not an integer\n");
        		json_decref(file);
        		return -1;
    		} else {
			fp->metadata.size = (size_t) json_integer_value(size);
		}
		json_decref(size);
	}	
	json_decref(file);

	return 0;
}






