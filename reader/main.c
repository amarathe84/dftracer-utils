#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "indexer.h"
#include "reader.h"

static long long file_size_bytes(const char *path)
{
    FILE *fp = fopen(path, "rb");
    if (!fp)
        return -1;
    fseeko(fp, 0, SEEK_END);
    long long sz = ftello(fp);
    fclose(fp);
    return sz;
}

static void print_usage(const char *prog_name)
{
    fprintf(stderr, "Usage: %s <file.gz> [OPTIONS]\n", prog_name);
    fprintf(stderr, "\nOptions:\n");
    fprintf(stderr, "  -s, --start <MB>           Start position in megabytes\n");
    fprintf(stderr, "  -e, --end <MB>             End position in megabytes\n");
    fprintf(stderr, "  -h, --help                 Show this help message\n");
    fprintf(stderr, "\nExamples:\n");
    fprintf(stderr, "  %s file.gz                     # Create index if needed\n", prog_name);
    fprintf(stderr, "  %s file.gz -s 1.0 -e 2.0        # Read 1MB-2MB range\n", prog_name);
    fprintf(stderr, "  %s file.gz -s 10.5 -e 11.0      # Read 10.5MB-11MB range\n", prog_name);
}

static int index_exists_and_valid(const char *idx_path)
{
    FILE *f = fopen(idx_path, "rb");
    if (!f)
        return 0; // File doesn't exist
    fclose(f);

    // Check if it has the required tables for chunk-based indexing
    sqlite3 *db;
    if (sqlite3_open(idx_path, &db) != SQLITE_OK)
    {
        return 0;
    }

    sqlite3_stmt *stmt;
    const char *sql = "SELECT name FROM sqlite_master WHERE type='table' AND "
                      "name IN ('chunks', 'metadata')";
    int table_count = 0;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK)
    {
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            table_count++;
        }
        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);
    return table_count >= 2; // Need both chunks and metadata tables
}

int main(int argc, char **argv)
{
    // Command line options
    double start_mb = -1;
    double end_mb = -1;

    // Define long options
    static struct option long_options[] = {{"start", required_argument, 0, 's'},
                                           {"end", required_argument, 0, 'e'},
                                           {"help", no_argument, 0, 'h'},
                                           {0, 0, 0, 0}};

    int opt;
    int option_index = 0;

    // Parse command line options
    while ((opt = getopt_long(argc, argv, "s:e:h", long_options, &option_index)) != -1)
    {
        switch (opt)
        {
        case 's':
            start_mb = atof(optarg);
            break;
        case 'e':
            end_mb = atof(optarg);
            break;
        case 'h':
            print_usage(argv[0]);
            return 0;
        case '?':
            print_usage(argv[0]);
            return 1;
        default:
            print_usage(argv[0]);
            return 1;
        }
    }

    // Check if we have the required file argument
    if (optind >= argc)
    {
        fprintf(stderr, "Error: Missing gzip file argument\n\n");
        print_usage(argv[0]);
        return 1;
    }

    const char *gz_path = argv[optind];

    // Validate argument combinations
    int has_byte_range = (start_mb != -1 || end_mb != -1);

    if (has_byte_range && (start_mb == -1 || end_mb == -1))
    {
        fprintf(stderr, "Error: Both --start and --end must be specified for MB range\n");
        return 1;
    }

    /* construct index path: "<gz_path>.idx" */
    size_t len = strlen(gz_path) + 5;
    char *idx_path = malloc(len);
    if (!idx_path)
    {
        perror("malloc");
        return 1;
    }
    snprintf(idx_path, len, "%s.idx", gz_path);

    /* Check if we need to create the index */
    int need_index = !index_exists_and_valid(idx_path);

    if (need_index)
    {
        printf("Index not found or invalid, creating index for %s...\n", gz_path);

        /* open SQLite DB for creation */
        sqlite3 *db;
        if (sqlite3_open(idx_path, &db) != SQLITE_OK)
        {
            fprintf(stderr, "Cannot create DB %s: %s\n", idx_path, sqlite3_errmsg(db));
            free(idx_path);
            return 1;
        }

        /* init schema */
        if (init_schema(db) != SQLITE_OK)
        {
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }

        /* get file info */
        long long bytes = file_size_bytes(gz_path);
        if (bytes < 0)
        {
            fprintf(stderr, "Cannot stat %s\n", gz_path);
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }
        long long mtime = 0; /* optionally get real mtime from stat() */

        /* insert or get file_id */
        sqlite3_stmt *st;
        if (sqlite3_prepare_v2(db,
                               "INSERT INTO files(logical_name, byte_size, "
                               "mtime_unix, sha256_hex) "
                               "VALUES(?, ?, ?, '') "
                               "ON CONFLICT(logical_name) DO UPDATE SET "
                               "byte_size=excluded.byte_size "
                               "RETURNING id;",
                               -1,
                               &st,
                               NULL) != SQLITE_OK)
        {
            fprintf(stderr, "Prepare failed: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }
        sqlite3_bind_text(st, 1, gz_path, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 2, bytes);
        sqlite3_bind_int64(st, 3, mtime);

        int rc = sqlite3_step(st);
        if (rc != SQLITE_ROW && rc != SQLITE_DONE)
        {
            fprintf(stderr, "Insert failed: %s\n", sqlite3_errmsg(db));
            sqlite3_finalize(st);
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }
        int file_id = sqlite3_column_int(st, 0);
        sqlite3_finalize(st);

        /* build the index with 32MiB stride */
        long long stride = 32LL * 1024 * 1024;
        int ret = build_gzip_index(db, file_id, gz_path, stride);
        if (ret != 0)
        {
            fprintf(stderr, "Index build failed for %s (error code: %d)\n", gz_path, ret);
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }

        printf("Index built successfully for %s\n", gz_path);
        sqlite3_close(db);
    }

    /* Handle read operations */
    if (has_byte_range)
    {
        /* open SQLite DB for reading */
        sqlite3 *db;
        if (sqlite3_open(idx_path, &db) != SQLITE_OK)
        {
            fprintf(stderr, "Cannot open DB %s: %s\n", idx_path, sqlite3_errmsg(db));
            free(idx_path);
            return 1;
        }

        char *output;
        size_t output_size;

        printf("Reading MB range [%.2f, %.2f] from %s...\n", start_mb, end_mb, gz_path);

        int ret = read_data_range_megabytes(db, gz_path, start_mb, end_mb, &output, &output_size);

        if (ret != 0)
        {
            fprintf(stderr, "Failed to read range from %s\n", gz_path);
            sqlite3_close(db);
            free(idx_path);
            return 1;
        }

        /* Output the extracted data to stdout */
        fwrite(output, 1, output_size, stdout);

        free(output);
        sqlite3_close(db);
    }

    free(idx_path);
    return 0;
}
