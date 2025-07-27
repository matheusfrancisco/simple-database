#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE];
  char email[COLUMN_EMAIL_SIZE];
} Row;

// to store this into some memory 
// sqlite uses a btree but I will do and array
// but we will keep rows into pages, grouped by pages
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
// thwat this means the layout of the page serialized row will look like this;
// column 1: id (4 bytes) offset 0
// column 2: username (32 bytes) offset 4
// column 3: email (255 bytes) offset 36
// the total size of the row is 291 bytes

void serialize_row(Row* source, void* dest) {
  memcpy(dest+ ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(dest + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(dest + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

//table structure points to pages of rows and keeps track of how many 
// rows are in the table

const uint32_t PAGE_SIZE = 4096; // 4KB pages
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE/ ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
  uint32_t num_rows;
  void* pages[TABLE_MAX_PAGES];// this should be a btree
}Table;
// making our page size 4kilobytes because its the same size as a page used
// in the virtual memory systems of most computer architectures.
// This means one page in our database corresponds to one page
//  used by the operation system.
//  The operating system will move pages in and out of memory as 
//  whole units instead of braking them up.

// The 100 pages is a arbitrary limit for now
// if you switch to a tree strcuture
// our database maximum size will only be limited by the maximum
// size of a file (Although we will still limit how many pages keep in memory at once)
//
// Rows should not cross page boundaries

void* row_slot(Table* table, uint32_t row_num) {

  uint32_t page_num = row_num/ ROWS_PER_PAGE;
  void * page = table->pages[page_num];
  if(page == NULL) {
    // allocate memory only when we try to acces a page;
     page = table->pages[page_num] = malloc(PAGE_SIZE);
  }
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return page + byte_offset;
}

typedef struct {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

void prompt() {
  printf("sqlite > "); 
  fflush(stdout);
}

//ssize_t getline(char **lineptr, size_t *n, FILE *stream);
void read_input(InputBuffer *input_buffer) {
  ssize_t bytes_read = getline(
    &(input_buffer->buffer),
    &(input_buffer->buffer_length),
    stdin
  );

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }
  input_buffer->input_length = bytes_read - 1; // Exclude newline character
  input_buffer->buffer[bytes_read -1] = 0;

}

void close_input(InputBuffer* input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

InputBuffer* new_input_bf() {
  InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;
  return input_buffer;
}


typedef enum {
  META_COMMAND_SUCCESS,
  //because c does not have exception
  META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum { 
  PREPARE_SUCCESS,
  //because c does not have exception
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_SYNTAX_ERROR
} PrepareResult;

MetaCommandResult do_meta_command(InputBuffer *input_buffer) {
  if(strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input(input_buffer);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
};

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT 
} StatementType;

typedef struct {
  StatementType type;
  Row row_to_insert;
} Statement;


PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement) {
  // compare a sequence
  if(strncmp(input_buffer->buffer, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;
    int args_assigned = sscanf(
      input_buffer->buffer, 
      "insert %d %s %s", 
      &(statement->row_to_insert.id),
      statement->row_to_insert.username, 
      statement->row_to_insert.email);
    if(args_assigned < 3) {
      return PREPARE_SYNTAX_ERROR;
    }

    return PREPARE_SUCCESS;
  }
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

typedef enum { 
  EXECUTE_SUCCESS,
  EXECUTE_TABLE_FULL } 
ExecuteResult;

void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
  if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }

  Row* row_to_insert = &(statement->row_to_insert);

  serialize_row(row_to_insert, row_slot(table, table->num_rows));
  table->num_rows += 1;

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
  Row row;
  for (uint32_t i = 0; i < table->num_rows; i++) {
    deserialize_row(row_slot(table, i), &row);
    print_row(&row);
  }
  return EXECUTE_SUCCESS;
}

ExecuteResult execute(Statement* statement, Table* table) {
   switch (statement->type) {
     case (STATEMENT_INSERT):
      return execute_insert(statement, table);
     case (STATEMENT_SELECT):
      return execute_select(statement, table);
   }
}
// initialize the table
Table* new_table() {
  Table* table = (Table*)malloc(sizeof(Table));
  table->num_rows = 0;
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    table->pages[i] = NULL;
  }
  return table;
}

void free_table(Table* table) {
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    free(table->pages[i]);
  }
  free(table);
}


int main(int argc, char* argv[]) {
  Table* table = new_table();
  InputBuffer* ip_bf = new_input_bf();
  while(1) {
    prompt();
    read_input(ip_bf);
    if (strncmp(ip_bf->buffer, ".", 1) == 0) {
      switch(do_meta_command(ip_bf)) {
        case(META_COMMAND_SUCCESS):
          continue;
        case(META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("Unrecognized command '%s'\n", ip_bf->buffer);
          continue; // skip empty input
      }
    }

    Statement statement;
    switch(prepare_statement(ip_bf, &statement)) {
      case (PREPARE_SUCCESS):
        break;
      case (PREPARE_SYNTAX_ERROR):
        printf("Syntax error for parse statement");
        continue;
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("Unrecognized keyword at start of '%s'\n", ip_bf->buffer);
        continue; // skip empty input
    }
    switch(execute(&statement, table)) {
      case (EXECUTE_SUCCESS):
        printf("Executed.\n");
        break;
      case (EXECUTE_TABLE_FULL):
        printf("Error: Table full.\n");
        continue; // skip empty input
    }

    printf("Executed.\n");

  }

}
