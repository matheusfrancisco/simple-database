#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1]; // + 1 to have a null terminator 
  char email[COLUMN_EMAIL_SIZE + 1]; //+ 1 to have a null terminator
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
  strncpy(dest + USERNAME_OFFSET, source->username, USERNAME_SIZE);
  strncpy(dest + EMAIL_OFFSET, source->email, EMAIL_SIZE);
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

//Disk
typedef struct {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages; // number of pages in the file
  void *pages[TABLE_MAX_PAGES];
}Pager;

typedef struct {
  uint32_t num_rows;
  uint32_t root_page_num; // the page number of the root node
  Pager* pager;
}Table;

typedef enum {
  NODE_INTERNAL, NODE_LEAF
} NodeType;
//Each node will correspond to one page. Internal nodes will 
//point to their children by storing the page number that stores 
//the child.
//
//Common Node Headers Layout
// every node will store what type of the node it is,
// whether or not it is the root node, and a pointer
// to its parent(to allow finding a nodes siblings). I define constants
// for the size and offset of every header field;
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
/*
 * Leaf Node Header Layout
 * how many cells they contain a cell is  a key value pair
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;
/*Leaf Node Body Layout*/

const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// accessing leaf node fields 
// return a pointer to the value in question
uint32_t* leaf_node_num_cells(void* node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void *leaf_node_cell(void* node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_OFFSET;
}

void* leaf_node_value(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_VALUE_OFFSET;
}


void initialize_leaf_node(void* node) { *leaf_node_num_cells(node) = 0; }


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


//get_pager
//We assume pages are saved one after the other in
//the database file: Page 0 at offset 0, 
//page 1 at offset 4096, page 2 at offset 8192,

void* get_page(Pager* pager, uint32_t page_num){
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
           TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    // Cache miss. Allocate memory and load from file.
    void* page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // We might save a partial page at the end of the file
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }

    if (page_num <= num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page;
    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1; // Update the number of pages
    }
  }

  return pager->pages[page_num];
}


//Create a cursor at the beginning of the table
//Create a cursor at the end of the table
//Access the row the cursor is pointing to
//Advance the cursor to the next row
typedef struct {
  Table* table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;//indicates a position one past the last element
}Cursor ;

Cursor* table_start(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = table->root_page_num; // Assuming root_page_num is the first page
  cursor->cell_num = 0; // Start at the first cell
  void* root_node = get_page(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->end_of_table = (num_cells == 0); // If there are no cells, we are at the end of the table
  return cursor;
}

Cursor* table_end(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = table->root_page_num;

  void* root_node = get_page(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->cell_num = num_cells; // Set to one past the last cell
  cursor->end_of_table = true; // Indicates a position one past the last element
  return cursor;
}

//lets insert into the btree the key/value pair
// will take a cursor as input to represent the position where the pair should be inserted
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
  void* node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  if(num_cells>= LEAF_NODE_MAX_CELLS ) {
    //Node is full
    printf("Leaf node full. Need to implement splitting.\n");
    exit(EXIT_FAILURE);
  }
  if (cursor->cell_num < num_cells) {
    // Make room for the new cell
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
             LEAF_NODE_CELL_SIZE);
    }
  }
  // Insert the new cell
  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));

}


void* cursor_value(Cursor* cursor) {

  uint32_t page_num = cursor->page_num;
  void* page = get_page(cursor->table->pager, page_num);
  return leaf_node_value(page,cursor->cell_num);
}
void cursor_advance(Cursor* cursor){

  uint32_t page_num = cursor->page_num;
  void*  node = get_page(cursor->table->pager, page_num);
  cursor->cell_num += 1;
  if(cursor->cell_num >= (*leaf_node_num_cells(node))) {
    cursor->end_of_table = true;
  }
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
  PREPARE_SYNTAX_ERROR,
  PREPARE_STRING_TOO_LONG,
  PREPARE_NEGATIVE_ID
} PrepareResult;

void pager_flush(Pager* pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

void db_close(Table* table) {
  Pager* pager = table->pager;

  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }


  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}

void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void print_leaf_node(void* node) {
  uint32_t num_cells = *leaf_node_num_cells(node);
  printf("leaf (size %d)\n", num_cells);
  for (uint32_t i = 0; i < num_cells; i++) {
    uint32_t key = *leaf_node_key(node, i);
    printf("  - %d : %d\n", i, key);
  }
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
  if(strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(table);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
    printf("Tree:\n");
    print_leaf_node(get_page(table->pager, 0));
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
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

PrepareResult prepare_insert(
  InputBuffer* input_buffer,
  Statement* statement) {
  char *keyword = strtok(input_buffer->buffer, " ");
  char *id_str = strtok(NULL, " ");
  char *username = strtok(NULL, " ");
  char *email = strtok(NULL, " ");
  if (id_str == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }
  int id = atoi(id_str);
  if(id < 0) {
    return PREPARE_NEGATIVE_ID;
  }
  if (strlen(username) > COLUMN_USERNAME_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }
  if (strlen(email)> COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }
  if (strlen(email) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }

  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement) {
  // compare a sequence
  if(strncmp(input_buffer->buffer, "insert", 6) == 0) {
    statement->type = STATEMENT_INSERT;
    return prepare_insert(input_buffer, statement);
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
  void* node = get_page(table->pager, table->root_page_num);
  if ((*leaf_node_num_cells(node)) >= LEAF_NODE_MAX_CELLS) {
    return EXECUTE_TABLE_FULL;
  }

  Row* row_to_insert = &(statement->row_to_insert);
  Cursor* cursor = table_end(table);

  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
  Cursor *cursor = table_start(table);
  Row row;
  while(!(cursor->end_of_table)){
    deserialize_row(cursor_value(cursor), &row);
    print_row(&row);
    cursor_advance(cursor);
  }
  free(cursor);
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

Pager* pager_open(const char* filename) {
  int fd = open(filename,
                O_RDWR |      // Read/Write mode
                    O_CREAT,  // Create file if it does not exist
                S_IWUSR |     // User write permission
                    S_IRUSR   // User read permission
                );

  if (fd == -1) {
    printf("Unable to open file %s\n", filename);
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);
  Pager* pager=malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = file_length / PAGE_SIZE;

  if(file_length % PAGE_SIZE != 0) {
    printf("DB file is not a whole number of pages. Corrupt file.\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL; // Initialize all pages to NULL
  }
  return pager;

}
// initialize the table
Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);
  uint32_t num_rows = pager->file_length / ROW_SIZE;

  Table* table = (Table*)malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0; // Start with the first page as the root
  if(pager->num_pages == 0) {
    //New database file, initialize page 0 as leaf node
    void* root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
  }
  return table;
}




int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  char* filename = argv[1];
  Table* table = db_open(filename);

  InputBuffer* ip_bf = new_input_bf();
  while(1) {
    prompt();
    read_input(ip_bf);
    if (strncmp(ip_bf->buffer, ".", 1) == 0) {
      switch(do_meta_command(ip_bf, table)) {
        case(META_COMMAND_SUCCESS):
          continue;
        case(META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("Unrecognized command '%s'\n", ip_bf->buffer);
          continue; // skip empty input
      }
    }

    Statement statement;
    printf("DEBUG: %s\n", ip_bf->buffer);
    switch(prepare_statement(ip_bf, &statement)) {
      case (PREPARE_SUCCESS):
        break;
      case (PREPARE_NEGATIVE_ID):
        printf("ID must be positive.\n");
        continue; // skip empty input
      case (PREPARE_STRING_TOO_LONG):
        printf("String is too long.\n");
        continue; // skip empty input
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


  }

}
