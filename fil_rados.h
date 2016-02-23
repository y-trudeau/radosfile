
#define METADATA_OBJECT_NAME metadata

/* Structure to perform the role of a file handle with rados */
/* Borrowed from os0file.h */
enum os_file_type_t {
	OS_FILE_TYPE_UNKNOWN = 0,
	OS_FILE_TYPE_FILE,			/* regular file
						(or a character/block device) */
	OS_FILE_TYPE_DIR,			/* directory */
	OS_FILE_TYPE_LINK			/* symbolic link */
};

struct rados_file_metadata_entry {
	char			*name;
	os_file_type_t		type;
	unsigned int		block_size;
	unsigned long long	total_size; /* in MySQL: ib_int64_t */
	/* could also have mtime, ctime, atime and perm, see struct os_file_stat_t in os0file.h */

};

struct rados_file_handle {
	struct rados_file_metadata_entry  metadata;
	unsigned long long	position; /* in MySQL: ib_int64_t */
};

typedef struct rados_file_handle FILErados_t;



