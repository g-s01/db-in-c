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

typedef enum
{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
}metaCommandResult;

typedef enum
{
	PREPARE_SUCCESS,
	PREPARE_UNRECOGNIZED_STATEMENT
}prepareResult;

typedef enum
{
	STATEMENT_INSERT,
	STATEMENT_SELECT
}statementType;

typedef struct
{
	statementType type;
}statement;

inputBuffer* new_input_buffer()
{
	inputBuffer * input_buffer = malloc(sizeof(inputBuffer));
	input_buffer->buffer = NULL;
	input_buffer->buffer_length = 0;
	input_buffer->input_length = 0;
	return input_buffer;
}

void close_input_buffer(inputBuffer* input_buffer)
{
	free(input_buffer->buffer);
	free(input_buffer);
}

metaCommandResult do_meta_command(inputBuffer * input_buffer)
{
	if(strcmp(input_buffer->buffer, ".exit") == 0)
	{
		close_input_buffer(input_buffer);	
		exit(EXIT_SUCCESS);
	}
	else
	{
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

prepareResult prepare_statement(inputBuffer* input_buffer, statement * exp)
{
	if(strncmp(input_buffer->buffer, "insert", 6) == 0)
	{
		exp->type = STATEMENT_INSERT;
		return PREPARE_SUCCESS;
	}
	else if(strcmp(input_buffer->buffer, "select") == 0)
	{
		exp->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(statement * exp)
{
	switch(exp->type)
	{
		case (STATEMENT_INSERT):
			printf("Insertion\n");
			break;
		case (STATEMENT_SELECT):
			printf("Selection\n");
			break;
	}
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

int main()
{
	inputBuffer * input_buffer = new_input_buffer();
	while(true)
	{
		print_prompt();	
		read_input(input_buffer);
		if(input_buffer->buffer[0] == '.')
		{
			switch(do_meta_command(input_buffer))
			{
				case (META_COMMAND_SUCCESS):
					continue;
				case (META_COMMAND_UNRECOGNIZED_COMMAND):
					printf("Unrecognized command '%s'\n", input_buffer->buffer);
					continue;
			}
		}
	
		statement exp;
		switch(prepare_statement(input_buffer, &exp))
		{
			case (PREPARE_SUCCESS):
				break;
			case (PREPARE_UNRECOGNIZED_STATEMENT):
				printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);       		
				continue;
		}
		execute_statement(&exp);
		printf("Executed.\n");
	}
}
