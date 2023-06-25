#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct
{
	char * buffer;
	size_t buffer_length;
	ssize_t input_length;
}inputBuffer;

inputBuffer* new_input_buffer()
{
	inputBuffer * input_buffer = malloc(sizeof(inputBuffer));
	input_buffer->buffer = NULL;
	input_buffer->buffer_length = 0;
	input_buffer->input_length = 0;
	return input_buffer;
}

void print_prompt()
{
	printf("db > ");
}

void read_input(inputBuffer* input_buffer)
{
	ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
	if(bytes_read <= 0)
	{
		printf("Error reading input\n");
		exit(EXIT_FAILURE);
	}
	// newline charachter ignored
	input_buffer->input_length = bytes_read-1;
	input_buffer->buffer[bytes_read-1] = 0;
}

void close_input_buffer(inputBuffer* input_buffer)
{
	free(input_buffer->buffer);
	free(input_buffer);
}

int main()
{
	inputBuffer * input_buffer = new_input_buffer();
	while(true)
	{
		print_prompt();	
		read_input(input_buffer);
		if(strcmp(input_buffer->buffer, ".exit") == 0)	
		{
			close_input_buffer(input_buffer);
			exit(EXIT_SUCCESS);
		}
		else
		{
			printf("Unrecognized command '%s'. \n", input_buffer->buffer);
		}
	}
}
