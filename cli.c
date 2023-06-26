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
	EXECUTE_DUPLICATE_KEY,
	EXECUTE_TABLE_FULL
}executeResult;

typedef struct
{
	int file_descriptor;
	uint32_t file_length;	
	uint32_t num_pages;
	void * pages[TABLE_MAX_PAGES];
}pager;

typedef struct 
{
	uint32_t root_page_num;
	pager * p;	
}table;

typedef struct
{
	table * t;	
	uint32_t page_num;
	uint32_t cell_num;
	bool end_of_table; // indicates the position one past the last element
}cursor;

typedef enum
{
	NODE_INTERNAL,
	NODE_LEAF
}nodeType;

// entry layout
const uint32_t ID_SIZE = size_of_attribute(row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET+ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET+USERNAME_SIZE;
// page layout
const uint32_t ROW_SIZE = ID_SIZE+USERNAME_SIZE+EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
// const uint32_t ROWS_PER_PAGE = PAGE_SIZE/ROW_SIZE;
// const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
// common node header layout
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
// leaf header layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;
// leaf body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS+1)/2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS+1)-LEAF_NODE_RIGHT_SPLIT_COUNT;
// internal node header layout
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;
// internal node body layout
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

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

void print_constants()
{
	printf("ROW_SIZE: %d\n", ROW_SIZE);
	printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
	printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
	printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
	printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
	printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

uint32_t get_unused_page_num(pager * p)
{
	return p->num_pages;
}

uint32_t * leaf_node_num_cells(void * node)
{
	return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void * leaf_node_cell (void * node, uint32_t cell_num)
{
	return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t * leaf_node_key (void * node, uint32_t cell_num)
{
	return leaf_node_cell(node, cell_num);
}

void * leaf_node_value (void * node, uint32_t cell_num)
{
	return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

uint32_t * internal_node_num_keys(void * node)
{
	return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t * internal_node_right_child(void * node)
{
	return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t * internal_node_cell(void * node, uint32_t cell_num)
{
	return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

nodeType get_node_type(void * node)
{
	uint8_t value = *((uint8_t *)(node + NODE_TYPE_OFFSET));
	return (nodeType)value;
}

uint32_t * internal_node_child(void * node, uint32_t child_num)
{
	uint32_t num_keys = *internal_node_num_keys(node);
	if(child_num > num_keys)
	{
		printf("Tried to access child_num %d > num_keys %d.\n", child_num, num_keys);
		exit(EXIT_FAILURE);
	}
	else if(child_num == num_keys) return internal_node_right_child(node);
	return internal_node_cell(node, child_num);
}

uint32_t * internal_node_key(void * node, uint32_t key_num)
{
	return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t get_node_max_key(void * node)
{
	switch(get_node_type(node))
	{
		case NODE_INTERNAL:	
			return *internal_node_key(node, *internal_node_num_keys(node)-1);
		case NODE_LEAF:
			return *leaf_node_key(node, *leaf_node_num_cells(node)-1);
	}
}

bool is_node_root(void * node)
{
	uint8_t value = *((uint8_t *)(node + IS_ROOT_OFFSET));
	return (bool)value;
}

void set_node_root(void * node, bool is_root)
{
	uint8_t value = is_root;	
	*((uint8_t *)(node + IS_ROOT_OFFSET)) = value;
}

void set_node_type(void * node, nodeType type)
{
	uint8_t value = type;
	*((uint8_t *)(node + NODE_TYPE_OFFSET)) = value;
}

void initialize_leaf_node (void * node)
{
	set_node_type(node, NODE_LEAF);
	set_node_root(node, false);
	*leaf_node_num_cells(node) = 0;
}

void initialize_internal_node (void * node)
{
	set_node_type(node, NODE_INTERNAL);
	set_node_root(node, false);
	*internal_node_num_keys(node) = 0;
}

void indent(uint32_t level)
{
	for(uint32_t i = 0; i<level; i++) printf(" ");
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
		if(page_num >= p->num_pages) p->num_pages = page_num+1;
	}
	return p->pages[page_num];
}

void print_tree(pager * p, uint32_t page_num, uint32_t indentation_level)
{
	void * node = get_page(p, page_num);
	uint32_t num_keys, child;
	switch(get_node_type(node))
	{
		case NODE_LEAF:	
			num_keys = *leaf_node_num_cells(node);
			indent(indentation_level);
			printf("- leaf (size %d)\n", num_keys);
			for(uint32_t i = 0; i<num_keys; i++)
			{
				indent(indentation_level+1);
				printf("- %d\n", *leaf_node_key(node, i));
			}
			break;
		case NODE_INTERNAL:
			num_keys = *internal_node_num_keys(node);
			indent(indentation_level);
			printf("- internal (size %d)\n", num_keys);
			for(uint32_t i = 0; i<num_keys; i++)
			{
				child = *internal_node_child(node, i);	
				print_tree(p, child, indentation_level+1);	
				indent(indentation_level+1);
				printf("- key %d\n", *internal_node_key(node, i));
			}
			child = *internal_node_right_child(node);
			print_tree(p, child, indentation_level+1);
			break;
	}
}

cursor * table_start(table * t)
{	
	cursor * c = malloc(sizeof(cursor));	
	c->t = t;	
	c->page_num = t->root_page_num;
	c->cell_num = 0;
	void * root_node = get_page(t->p, t->root_page_num);
	uint32_t num_cells = *leaf_node_num_cells(root_node);
	c->end_of_table = (num_cells == 0);
	return c;
}

/*
return the position of the given key
if key is not present, return the position where it should be inserted
*/

cursor * leaf_node_find(table * t, uint32_t page_num, uint32_t key)
{
	void * node = get_page(t->p, page_num);
	uint32_t num_cells = *leaf_node_num_cells(node);
	cursor * c = malloc(sizeof(cursor));
	c->t = t;
	c->page_num = page_num;
	// bin search
	uint32_t l = 0, r = num_cells;
	while(l+1 < r)
	{
		uint32_t mid = (l+r)/2;
		uint32_t key_at_mid = *leaf_node_key(node, mid);
		if(key >= key_at_mid) l = mid;	
		else r = mid;
	}	
	c->cell_num = l;	
	return c;
}

cursor * table_find(table * t, uint32_t key)
{
	uint32_t root_page_num = t->root_page_num;
	void * root_node = get_page(t->p, root_page_num);
	if(get_node_type(root_node) == NODE_LEAF) return leaf_node_find(t, root_page_num, key);
	else	
	{
		printf("search node not implemented yet.\n");
		exit(EXIT_FAILURE);
	}
}

void cursor_advance(cursor * c)
{
	uint32_t page_num = c->page_num;	
	void * node = get_page(c->t->p, page_num);
	c->cell_num += 1;
	if(c->cell_num >= (*leaf_node_num_cells(node))) c->end_of_table = true;
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
	uint32_t page_num = c->page_num;
	void * page = get_page(c->t->p, page_num);
	return leaf_node_value(page, c->cell_num);
}

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
	p->num_pages = file_length / PAGE_SIZE;
	if(file_length % PAGE_SIZE != 0)
	{
		printf("DB file is not a whole number of pages. Corrupt file. \n");
		exit(EXIT_FAILURE);
	}
	for(uint32_t i = 0; i<TABLE_MAX_PAGES; i++) p->pages[i] = NULL;
	return p;
}

table * db_open(const char * filename)
{
	pager * p = pager_open(filename);
	table * t = malloc(sizeof(table));
	t->p = p;
	t->root_page_num = 0;
	if(p->num_pages == 0)
	{
		// new db file, make page 0 as leaf node
		void * root_node = get_page(p, 0);
		initialize_leaf_node(root_node);
		set_node_root(root_node, true);
	}
	return t;
}

void create_new_root(table * t, uint32_t right_child_page_num)
{
	/*
		handle splitting the root
		old root copied to new page, becomes the left child
		address of right child passed in 
		re-initialize root page to contain the new root node
		new root node points to two children
	*/
	void * root = get_page(t->p, t->root_page_num);
	void * right_child = get_page(t->p, right_child_page_num);
	uint32_t left_child_page_num = get_unused_page_num(t->p);
	void * left_child = get_page(t->p, left_child_page_num);
	// left child has data copied from old root
	memcpy(left_child, root, PAGE_SIZE);	
	set_node_root(left_child, false);
	// root node is a new internal node with one key and two children
	initialize_internal_node(root);	
	set_node_root(root, true);
	*internal_node_num_keys(root) = 1;
	*internal_node_child(root, 0) = left_child_page_num;
	uint32_t left_child_max_key = get_node_max_key(left_child);
	*internal_node_key(root, 0) = left_child_max_key;
	*internal_node_right_child(root) = right_child_page_num;
}

void pager_flush(pager * p, uint32_t page_num)
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
	ssize_t bytes_written = write(p->file_descriptor, p->pages[page_num], PAGE_SIZE);
	if(bytes_written == -1)
	{
		printf("Error writing: %d.\n", errno);
		exit(EXIT_FAILURE);
	}
}

void db_close(table * t)
{
	pager * p = t->p;
	// uint32_t num_full_pages = t->num_rows/ROWS_PER_PAGE;
	for(uint32_t i = 0; i<p->num_pages; i++)
	{
		if(p->pages[i] == NULL) continue;	
		pager_flush(p, i);
		free(p->pages[i]);	
		p->pages[i] = NULL;	
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

void leaf_node_split_and_insert(cursor * c, uint32_t key, row * value)
{
	/*
		create a new node and move half the cells over
		insert the new value in one of the two nodes
		update parent or create a new parent
	*/
	void * old_node = get_page(c->t->p, c->page_num);
	uint32_t new_page_num = get_unused_page_num(c->t->p);
	void * new_node = get_page(c->t->p, new_page_num);
	initialize_leaf_node(new_node);
	/*
		all existing keys plus new key should be divided 
		evenly between old(left) and new(right) nodes
		starting from the right, move each key to correct position
	*/
	for(int32_t i = LEAF_NODE_MAX_CELLS; i>=0; i--)
	{
		void * destination_node;
		if(i >= LEAF_NODE_LEFT_SPLIT_COUNT) destination_node = new_node;
		else destination_node = old_node;
		uint32_t index_within_node = i%LEAF_NODE_LEFT_SPLIT_COUNT;
		void * destination = leaf_node_cell(destination_node, index_within_node);
		if(i == c->cell_num) serialize_row(value, destination);
		else if(i > c->cell_num) memcpy(destination, leaf_node_cell(old_node, i-1), LEAF_NODE_CELL_SIZE);
		else memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);  
	}
	// update cell count on both leaf sides
	*(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
	*(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;
	if(is_node_root(old_node)) return create_new_root(c->t, new_page_num);
	else 
	{
		printf("Need to implement updating parent after split");
		exit(EXIT_FAILURE);
	}
}

void leaf_node_insert(cursor * c, uint32_t key, row * value)
{
	void * node = get_page(c->t->p, c->page_num);
	uint32_t num_cells = *leaf_node_num_cells(node);
	if(num_cells >= LEAF_NODE_MAX_CELLS)	
	{
		// node full
		leaf_node_split_and_insert(c, key, value);
		return;
	}
	if(c->cell_num < num_cells)
	{
		// make room for new cell
		for(uint32_t i = num_cells; i>c->cell_num; i--) memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i-1), LEAF_NODE_CELL_SIZE);
	}
	*(leaf_node_num_cells(node)) += 1;
	*(leaf_node_key(node, c->cell_num)) = key;
	serialize_row(value, leaf_node_value(node, c->cell_num));
}

metaCommandResult do_meta_command(inputBuffer * input_buffer, table * t)
{
	if(strcmp(input_buffer->buffer, ".exit") == 0)
	{
		close_input_buffer(input_buffer);	
		db_close(t);
		exit(EXIT_SUCCESS);
	}
	else if(strcmp(input_buffer->buffer, ".constants") == 0)
	{
		printf("Constants:\n");
		print_constants();
		return META_COMMAND_SUCCESS;
	}
	else if(strcmp(input_buffer->buffer, ".btree") == 0)
	{
		printf("Tree:\n");	
		print_tree(t->p, 0, 0);
		return META_COMMAND_SUCCESS;
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
	void * node = get_page(t->p, t->root_page_num);
	uint32_t num_cells = (*leaf_node_num_cells(node));
	row * row_to_insert = &(exp->row_to_insert);
	uint32_t key_to_insert = row_to_insert->id;
	cursor * c = table_find(t, key_to_insert);
	if(c->cell_num < num_cells)
	{
		uint32_t key_at_index = *leaf_node_key(node, c->cell_num);
		if(key_at_index == key_to_insert) return EXECUTE_DUPLICATE_KEY;
	}
	leaf_node_insert(c, row_to_insert->id, row_to_insert);
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
			case (EXECUTE_DUPLICATE_KEY):	
				printf("Error: Duplicate key.\n");
				break;
			case (EXECUTE_TABLE_FULL):
				printf("Error: Table full.\n");
				break;
		}
	}
}
