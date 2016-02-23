#include <stdio.h>
#include <string.h>
#include <rados/librados.h>
#include <stdlib.h>

#define EXIT_FAILURE 1

int main (int argc, const char **argv)
{

        /* Declare the cluster handle and required arguments. */
        rados_t cluster;
        char cluster_name[] = "ceph";
        char user_name[] = "client.mysqlrados";
        uint64_t flags;

        /* Initialize the cluster handle with the "ceph" cluster name and the "client.admin" user */
        int err;
        err = rados_create2(&cluster, cluster_name, user_name, flags);

        if (err < 0) {
                fprintf(stderr, "%s: Couldn't create the cluster handle! %s\n", argv[0], strerror(-err));
                exit(EXIT_FAILURE);
        } else {
                printf("\nCreated a cluster handle.\n");
        }


        /* Read a Ceph configuration file to configure the cluster handle. */
        err = rados_conf_read_file(cluster, "/home/ubuntu/test-rados/ceph-mysqlrados.conf");
        if (err < 0) {
                fprintf(stderr, "%s: cannot read config file: %s\n", argv[0], strerror(-err));
                exit(EXIT_FAILURE);
        } else {
                printf("\nRead the config file.\n");
        }

        /* Read command line arguments */
        err = rados_conf_parse_argv(cluster, argc, argv);
        if (err < 0) {
                fprintf(stderr, "%s: cannot parse command line arguments: %s\n", argv[0], strerror(-err));
                exit(EXIT_FAILURE);
        } else {
                printf("\nRead the command line arguments.\n");
        }

        /* Connect to the cluster */
        err = rados_connect(cluster);
        if (err < 0) {
                fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-err));
                exit(EXIT_FAILURE);
        } else {
                printf("\nConnected to the cluster.\n");
        }

        rados_ioctx_t io;
        char *poolname = "mysqlpool";

        err = rados_ioctx_create(cluster, poolname, &io);
        if (err < 0) {
                fprintf(stderr, "%s: cannot open rados pool %s: %s\n", argv[0], poolname, strerror(-err));
                rados_shutdown(cluster);
                exit(EXIT_FAILURE);
        } else {
                printf("\nCreated I/O context. %lu\n", sizeof(io));
        }

        char *str,key[6];
	int  i;
	strcpy(key,"test");	

	str = (char *) malloc(16384);
	memset(str,1,16384);

	/* Write data to the cluster synchronously. */
        err = rados_write_full(io, key, str, strlen(str));
       	if (err < 0) {
               	fprintf(stderr, "Cannot write object to pool %s: %s\n", poolname, strerror(-err));
                rados_ioctx_destroy(io);
       	        rados_shutdown(cluster);
               	exit(1);
        } else {
       	        printf("\nWrote %s",key);
       	}
	
	rados_ioctx_destroy(io);
	rados_shutdown(cluster);
}
