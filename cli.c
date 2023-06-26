#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

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
	char username[COLUMN_USERNAME_SIZE+1];
	char email[COLUMN_EMAIL_SIZE+1];
}row;

typedef enum
{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
}metaCommandResult;

typedef enum
{
	PREPARE_SUCCESS,
	PREPARE_NEGATIVE_ID,
	PREPARE_STRING_TOO_LONG,
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
	int file_descriptor;
	uint32_t file_length;	
	void * pages[TABLE_MAX_PAGES];
}pager;

typedef struct 
{
	uint32_t num_rows;
	pager * p;	
}table;

typedef struct
{
	table * t;	
	uint32_t row_num;	
	bool end_of_table; // indicates the position one past the last element
}cursor;

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

cursor * table_start(table * t)
{	
	cursor * c = malloc(sizeof(cursor));	
	c->t = t;	
	c->row_num = 0;
	c->end_of_table = (t->num_rows == 0);
	return c;
}

void cursor_advance(cursor * c)
{
	c->row_num += 1;
	if(c->row_num >= c->t->num_rows) c->end_of_table = true;
}

cursor * table_end(table * t)
{
	cursor * c = malloc(sizeof(cursor));
	c->t = t;
	c->row_num = t->num_rows;
	c->end_of_table = true;
	return c;
}

void * get_page(pager * p, uint32_t page_num)
{
	if(page_num > TABLE_MAX_PAGES)
	{
		printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
		exit(EXIT_FAILURE);
	}
	if(p->pages[page_num] == NULL)
	{
		// cache miss, allocate memory and load file
		void * page = malloc(PAGE_SIZE);
		uint32_t num_pages = p->file_length / PAGE_SIZE;	
		// we might save a partial page at the end of the file
		if(p->file_length % PAGE_SIZE) num_pages += 1;
		if(page_num <= num_pages)
		{
			lseek(p->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);	
			ssize_t bytes_read = read(p->file_descriptor, page, PAGE_SIZE);
			if(bytes_read == -1)
			{
				printf("Error reading file: %d\n", errno);
				exit(EXIT_FAILURE);
			}
		} 
		p->pages[page_num] = page;	
	}
	return p->pages[page_num];
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

void * cursor_value(cursor * c)
{
	uint32_t row_num = c->row_num;
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void * page = get_page(c->t->p, page_num);
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	return page + byte_offset;
}

/*
table * new_table()
{
	table * t = (table *)malloc(sizeof(table));
	t->num_rows = 0;
	for(uint32_t i = 0; i<TABLE_MAX_PAGES; i++) t->pages[i] = NULL;
	return t;
}
*/

pager * pager_open(const char * filename)
{
	int fd = open(filename, O_RDWR|O_CREAT, S_IWUSR|S_IRUSR);
	if(fd == -1)
	{
		printf("Unable to open file.\n");
		exit(EXIT_FAILURE);
	}
	off_t file_length = lseek(fd, 0, SEEK_END);
	pager * p = malloc(sizeof(pager));
	p->file_descriptor = fd;
	p->file_length = file_length;
	for(uint32_t i = 0; i<TABLE_MAX_PAGES; i++) p->pages[i] = NULL;
	return p;
}

/*
void free_table(table * t)
{
	for(int i = 0; t->pages[i]; i++) free(t->pages[i]);
	free(t);
}
*/

table * db_open(const char * filename)
{
	pager * p = pager_open(filename);
	uint32_t num_rows = p->file_length / ROW_SIZE;
	table * t = malloc(sizeof(table));
	t->p = p;
	t->num_rows = num_rows;
	return t;
}

void pager_flush(pager * p, uint32_t page_num, uint32_t size)
{
	if(p->pages[page_num] == NULL)
	{
		printf("Tried to flush null page.\n");	
		exit(EXIT_FAILURE);
	}
	off_t offset = lseek(p->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
	if(offset == -1)
	{
		printf("Error seeking: %d.\n", errno);	
		exit(EXIT_FAILURE);
	}
	ssize_t bytes_written = write(p->file_descriptor, p->pages[page_num], size);
	if(bytes_written == -1)
	{
		printf("Error writing: %d.\n", errno);
		exit(EXIT_FAILURE);
	}
}

void db_close(table * t)
{
	pager * p = t->p;
	uint32_t num_full_pages = t->num_rows/ROWS_PER_PAGE;
	for(uint32_t i = 0; i<num_full_pages; i++)
	{
		if(p->pages[i] == NULL) continue;	
		pager_flush(p, i, PAGE_SIZE);
		free(p->pages[i]);	
		p->pages[i] = NULL;	
	}
	// there may be a partial page to write to the eof 
	uint32_t num_additional_rows = t->num_rows % ROWS_PER_PAGE;	
	if(num_additional_rows > 0)
	{
		uint32_t page_num = num_full_pages;
		if(p->pages[page_num] != NULL)
		{
			pager_flush(p, page_num, num_additional_rows * ROW_SIZE);
			free(p->pages[page_num]);
			p->pages[page_num] = NULL;
		}
	}
	int result = close(p->file_descriptor);
	if(result == -1)
	{
		printf("Error closing db file.\n");
		exit(EXIT_FAILURE);	
	}
	for(uint32_t i = 0; i<TABLE_MAX_PAGES; i++)	
	{
		void * page = p->pages[i];
		if(page)
		{
			free(page);	
			p->pages[i] = NULL;
		}
	}
	free(p);
	free(t);
}

metaCommandResult do_meta_command(inputBuffer * input_buffer, table * t)
{
	if(strcmp(input_buffer->buffer, ".exit") == 0)
	{
		close_input_buffer(input_buffer);	
		db_close(t);
		exit(EXIT_SUCCESS);
	}
	else
	{
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

prepareResult prepare_insert(inputBuffer * input_buffer, statement * exp)
{
	exp->type = STATEMENT_INSERT;

	char * keyword = strtok(input_buffer->buffer, " ");
	char * id_string = strtok(NULL, " ");
	char * username = strtok(NULL, " ");
	char * email = strtok(NULL, " ");
	
	if(id_string == NULL || username == NULL || email == NULL) return PREPARE_SYNTAX_ERROR;
	
	int id = atoi(id_string);
	if(id < 0) return PREPARE_NEGATIVE_ID;
	if(strlen(username) > COLUMN_USERNAME_SIZE)	return PREPARE_STRING_TOO_LONG;
	if(strlen(email) > COLUMN_EMAIL_SIZE) return PREPARE_STRING_TOO_LONG;
	
	exp->row_to_insert.id = id;
	strcpy(exp->row_to_insert.username, username);	
	strcpy(exp->row_to_insert.email, email);
	
	return PREPARE_SUCCESS;
}

prepareResult prepare_statement(inputBuffer* input_buffer, statement * exp)
{
	if(strncmp(input_buffer->buffer, "insert", 6) == 0) return prepare_insert(input_buffer, exp);
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
	cursor * c = table_end(t);
	serialize_row(row_to_insert, cursor_value(c));
	t->num_rows += 1;
	free(c);
	return EXECUTE_SUCCESS;
}

executeResult execute_select(statement * exp, table * t)
{
	cursor * c = table_start(t);
	row r;	
	while(!(c->end_of_table))
	{
		deserialize_row(cursor_value(c), &r);
		print_row(&r);
		cursor_advance(c);
	}
	free(c);
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

int main(int argc, char * argv[])
{
	inputBuffer * input_buffer = new_input_buffer();
	if(argc < 2)
	{
		printf("Must supply a database filename.\n");	
		exit(EXIT_FAILURE);
	}
	
	char * filename = argv[1];	
	table * t = db_open(filename);
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
			case (PREPARE_NEGATIVE_ID):
				printf("ID must be positive.\n");
				continue;	
			case (PREPARE_STRING_TOO_LONG):
				printf("String is too long.\n");
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
