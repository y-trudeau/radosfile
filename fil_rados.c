#include <stdio.h>
#include <string.h>
#include <rados/librados.h>
#include <stdlib.h>
#include <jansson.h>

#include "fil_rados.h"

json_t *metadata_json = NULL;
json_error_t error_json;

rados_ioctx_t rados_io_context;

fil_aio_read() {

}

fil_aio_wait() {

}

fil_aio_write() {

}

fil_close() {

}

/* 	
	Open a file and create it if it doesn't exist
	return 0 if successful, 1 if an error occurred 
*/
int fil_open_create(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t* type, /* file object type, seen enum def */
	FILErados_t* fp) /* Pseudo file handle pointer */ 
	{  

	/* checking if the path exists in the metadata */
	if (! _fil_exists_in_metadata(filepath,type)) {
		/* Adding the path to the metadata */
		if (_fil_add_metadata(filepath,type)) {
			return 1;
		}
	}
	
	return fil_open(filepath, fp);
}

fil_delete_file() {

}

fil_flush() {

}


/* 	
	Open a file
	return 0 if successful, 1 if an error occurred, 2 if file already exists 
*/
fil_open() 
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t* type, /* file object type, seen enum def */
	FILErados_t* fp) /* Pseudo file handle pointer */ 
{  
	/* checking if the path exists in the metadata */
	if (! _fil_exists_in_metadata(filepath,type)) {
		fprintf(stderr, "File path does not exist\n");
		return 1;
	}

	char *metadata;
	metadata = 
}

fil_read() {

}

fil_write() {

}

/* not needed for now */
fil_update_atime() {

}

/* not needed for now */
fil_update_mtime() {

}

fil_update_size() {

}

/* 	
	(pseudoPrivate) Load the metadata in memory
	return 0 if the path doesn't not exist, 1 if it does 
*/
int _fil_load_metadata_json(
	) 
{
	/* Is it already loaded */
	if (metadata_json) {
		return 0;
	}

	/* test if ioctx is set */
	rados_pool_stat_t pstat;
	if (rados_ioctx_pool_stat(rados_io_context,&pstat) < 0) {
		fprintf(stderr, "Error accessing Ceph, invalid IO context\n");
		return 1;
	}

	/* read the metadata object stat */
	uint64_t	metadata_size;
	time_t		metadata_mtime;
	if (rados_stat(rados_io_context,METADATA_OBJECT_NAME,&metadata_size,&metadata_mtime)) {
		fprintf(stderr, "Error stating Metadata\n");
		return 1;
	}

	/* Allocate the buffer for the metadata */	
	char		*bufmetadata;
	bufmetadata = (char *) malloc(metadata_size);
	if (!bufmetadata) {
		fprintf(stderr, "Error allocating memory for Metadata buffer\n");
		return 1;
	}

	/* Read the metadata object */
	if (rados_read(rados_io_context,bufmetadata,metadata_size,0) < 0) {
		fprintf(stderr, "Error reading metadata from rados\n");
		free(bufmetadata);
		return 1;
	}

	/* parse in json */
	metadata_json = json_loads(bufmetadata, 0, &error_json);
	if(!metadata_json) {
    		fprintf(stderr, "Error loading json on line %d: %s\n", error_json.line, error_json.text);
		free(bufmetadata);
    		return 1;
	}
	
	/* Free the text buffer */
	free(bufmetadata);

	return 0;
	
}


/* 	
	(pseudoPrivate) Load the metadata in memory
	return 0 if successful, 1 if not
*/
int _fil_update_metadata_json() {
	/* Is it already loaded */
	if (!metadata_json) {
		/* no... so shouldn't save */
		return 1;
	}

	/* test if ioctx is set */
	rados_pool_stat_t pstat;
	if (rados_ioctx_pool_stat(rados_io_context,&pstat) < 0) {
		fprintf(stderr, "Error accessing Ceph, invalid IO context\n");
		return 1;
	}

	char *buffer;
	buffer = json_dumps(metadata_json,JSON_COMPACT);

	if (!buffer) {
		fprintf(stderr, "Error dumping the internal metadata json\n");
		return 1;
	}

	if (rados_write_full(rados_io_context, METADATA_OBJECT_NAME, buffer, strlen(buffer)) < 0) {
		fprintf(stderr, "Error writing the metadata object to ceph\n");
		free(buffer);
		return 1;
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
	os_file_type_t type, /* file object type, seen enum def */
	) 
{
	/* is the metadata json loaded? */
	if (!metadata_json) {
		_fil_load_metadata_json();	
	}
	
	if(!json_is_array(metadata_json)) {
    		fprintf(stderr, "error: metadata_json is not an array\n");
		json_decref(metadata_json);
    		return 1;
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
	size_t blockSize
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

	json_t jsonObj = json_object();
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
        os_file_type_t type, /* file object type, seen enum def */
        )
{
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

_fil_get_file_metadata() {

}






