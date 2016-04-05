#include <jansson.h>

/* Structure to perform the role of a file handle with rados */
/* Borrowed from os0file.h */
enum os_file_type {
	OS_FILE_TYPE_UNKNOWN = 0,
	OS_FILE_TYPE_FILE,			/* regular file
						(or a character/block device) */
	OS_FILE_TYPE_DIR,			/* directory */
	OS_FILE_TYPE_LINK			/* symbolic link */
};

typedef enum os_file_type os_file_type_t;

struct rados_file_metadata_entry {
	char			*name;
	os_file_type_t		type;
	unsigned int		block_size;
	unsigned long long	size; /* in MySQL: ib_int64_t */
	unsigned int		deleted; /* 0 = not deleted, 1 = deleted */
	unsigned int		n_ref; /* number of references to the file, important for deletions */
	/* could also have mtime, ctime, atime and perm, see struct os_file_stat_t in os0file.h */

};

struct rados_file_handle {
	struct rados_file_metadata_entry  metadata;
	unsigned long long	position; /* in MySQL: ib_int64_t */
};

typedef struct rados_file_handle FILErados_t;

int fil_rados_init(
	const char* cluster_name, /* name of the cluster */
	const char* user_name, /* auth user for cephx */
	const char* pool_name, /* data pool */
	const char* conf_file /* configuration file */
	);

void fil_rados_destroy();

int fil_close(FILErados_t* fp);

FILErados_t* fil_open_create(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type, /* file object type, seen enum def */
	size_t block_size /* block size to use in rados */
	);
    
int fil_delete_file(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type /* file object type, seen enum def */
);


void fil_flush();

FILErados_t* fil_open( 
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type /* file object type, seen enum def */
);

ssize_t fil_read(
	FILErados_t*    fp,	/* handle to a file */
	void*		buf,	/* buffer where to read */
	size_t		len,	/* number of bytes to read */
	size_t		offset  /* offset from where to start reading */
);

int fil_write(	
    FILErados_t*    fp,	/* handle to a file */
	void*		buf,	/* buffer where to get data to write */
	size_t		len,    /* number of bytes to write */
	size_t		offset  /* offset from where to start reading */
    );
    
int _fil_get_block_size(
	json_t *file   /* json file element */
	);
    
int _fil_get_n_ref(
	json_t *file   /* json file element */
	);
    
int _fil_set_n_ref(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type,
    int delta  /* typically 1 or -1 */
	);
    

int _fil_increment_n_ref(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type
	);

int _fil_decrement_n_ref(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type
	);

unsigned int _fil_is_deleted(
	json_t *file   /* json file element */
	);
    
int _fil_update_size(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type, /* file object type, seen enum def */
	size_t new_size /* new file size */
	);
    
int _fil_set_deleted(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type
	);
    
int _fil_delete_rados_objects(
    const char* filepath,  /* path of the file */
    const unsigned int block_size 
    );

int _fil_load_metadata_json();

int _fil_update_metadata_json();

int _fil_find_in_metadata(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type /* file object type, seen enum def */
	);
    
int _fil_add_file_metadata(
	char* filepath,   /* file path like sbtest/sbtest.ibd */
	os_file_type_t type, /* file object type, seen enum def */
	size_t size,  /* Size of the file */
	size_t blockSize /* blockSize */
	);
    
int _fil_rm_file_metadata(
        char* filepath,   /* file path like sbtest/sbtest.ibd */
        os_file_type_t type /* file object type, seen enum def */
        );
        
json_t* _fil_get_json_metadata(
        char* filepath,   /* file path like sbtest/sbtest.ibd */
        os_file_type_t type /* file object type, seen enum def */
        );
        

int _fil_get_file_metadata(
        char* filepath,   /* file path like sbtest/sbtest.ibd */
        os_file_type_t type, /* file object type, seen enum def */
        FILErados_t*	fp  /* rados file FILE struct */
        );
        

