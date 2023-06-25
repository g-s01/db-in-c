#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255 
#define TABLE_MAX_PAGES 100

typedef struct
{
	char * buffer;
	size_t buffer_length;
	ssize_t input_length;
}inputBuffer;

typedef struct 
{
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE];
	char email[COLUMN_EMAIL_SIZE];
}row;

typedef enum
{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
}metaCommandResult;

typedef enum
{
	PREPARE_SUCCESS,
	PREPARE_SYNTAX_ERROR, 
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
	row row_to_insert; // only used by insert statement
}statement;

typedef enum
{
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
}executeResult;

typedef struct 
{
	uint32_t num_rows;
	void * pages[TABLE_MAX_PAGES];
}table;

const uint32_t ID_SIZE = size_of_attribute(row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET+ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET+USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE+USERNAME_SIZE+EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE/ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

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

void print_row(row * r)
{
	printf("(%d, %s, %s)\n", r->id, r->username, r->email);
}

void serialize_row(row * src, void * dest)
{
	memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);
	memcpy(dest + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);
	memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
}

void deserialize_row(void * src, row* dest)
{
	memcpy(&(dest->id), src+ID_OFFSET, ID_SIZE);
	memcpy(&(dest->username), src+USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(dest->email), src+EMAIL_OFFSET, EMAIL_SIZE);
}

void * row_slot(table * t, uint32_t row_num)
{
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void * page = t->pages[page_num];
	if(page == NULL)
	{
		// allocating memory only when trying to access the page
		page = t->pages[page_num] = malloc(PAGE_SIZE);
	}
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	return page + byte_offset;
}

table * new_table()
{
	table * t = (table *)malloc(sizeof(table));
	t->num_rows = 0;
	for(uint32_t i = 0; i<TABLE_MAX_PAGES; i++) t->pages[i] = NULL;
	return t;
}

void free_table(table * t)
{
	for(int i = 0; t->pages[i]; i++) free(t->pages[i]);
	free(t);
}

metaCommandResult do_meta_command(inputBuffer * input_buffer, table * t)
{
	if(strcmp(input_buffer->buffer, ".exit") == 0)
	{
		close_input_buffer(input_buffer);	
		free_table(t);
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
		int args_assigned = sscanf(input_buffer->buffer, "insert %d %s %s", &(exp->row_to_insert.id),
		exp->row_to_insert.username, exp->row_to_insert.email);
		if(args_assigned < 3) return PREPARE_SYNTAX_ERROR;
		return PREPARE_SUCCESS;
	}
	else if(strcmp(input_buffer->buffer, "select") == 0)
	{
		exp->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	return PREPARE_UNRECOGNIZED_STATEMENT;
}

executeResult execute_insert(statement * exp, table * t)
{
	if(t->num_rows >= TABLE_MAX_ROWS) return EXECUTE_TABLE_FULL;
	row * row_to_insert = &(exp->row_to_insert);
	serialize_row(row_to_insert, row_slot(t, t->num_rows));
	t->num_rows += 1;
	return EXECUTE_SUCCESS;
}

executeResult execute_select(statement * exp, table * t)
{
	row r;	
	for(uint32_t i = 0; i<t->num_rows; i++)
	{
		deserialize_row(row_slot(t, i), &r);
		print_row(&r);
	}
	return EXECUTE_SUCCESS;
}

executeResult execute_statement(statement * exp, table * t)
{
	switch(exp->type)
	{
		case (STATEMENT_INSERT):
			return execute_insert(exp, t);		
		case (STATEMENT_SELECT):
			return execute_select(exp, t);
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
	table * t = new_table();
	while(true)
	{
		print_prompt();	
		read_input(input_buffer);
		if(input_buffer->buffer[0] == '.')
		{
			switch(do_meta_command(input_buffer, t))
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
			case (PREPARE_SYNTAX_ERROR):
				printf("Syntax error. Could not parse statement.\n");
				continue;
			case (PREPARE_UNRECOGNIZED_STATEMENT):
				printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);       		
				continue;
		}
		switch (execute_statement(&exp, t)) 
		{
			case (EXECUTE_SUCCESS):
				printf("Executed.\n");
				break;
			case (EXECUTE_TABLE_FULL):
				printf("Error: Table full.\n");
				break;
		}
	}
}
