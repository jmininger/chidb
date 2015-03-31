#include <check.h>

#ifndef TEST_DIR
#define TEST_DIR "./tests/"
#endif

#define DATABASES_DIR TEST_DIR "files/databases/"
#define GENERATED_DIR TEST_DIR "files/generated/"
#define VDBE_PROGRAMS_DIR TEST_DIR "files/vdbe-programs/"
#define TMPFILE_TEMPLATE GENERATED_DIR "tmpfile-XXXXXX"

char *database_file_path(const char *fname);
char *generated_file_path(const char *fname);


char* create_copy(const char *src, const char *dst);
void delete_copy(char *f);
char* create_tmp_file();
void delete_tmp_file(char *f);
