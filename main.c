#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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


// 
typedef enum {
  META_COMMAND_SUCCESS,
  //because c does not have exception
  META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum { 
  PREPARE_SUCCESS,
  //because c does not have exception
  PREPARE_UNRECOGNIZED_STATEMENT  
} PrepareResult;

MetaCommandResult do_meta_command(InputBuffer *input_buffer) {

  return META_COMMAND_UNRECOGNIZED_COMMAND;
};

int main() {
  InputBuffer* ip_bf = new_input_bf();
  while(1) {
    prompt();
    read_input(ip_bf);
    if(strcmp(ip_bf->buffer, ".exit") == 0) {
      close_input(ip_bf);
      exit(EXIT_SUCCESS);
    } else {
      printf("Unrecognized command %s\n", ip_bf->buffer);
    }
  }

}
