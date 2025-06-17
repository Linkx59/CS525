CC = gcc
CFLAGS = -Wall -g
TARGET = test_assign2_1
TARGET2 = test_assign2_2  # New target name

# Source files for original target
SRC = dberror.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c test_assign2_1.c
OBJ = $(SRC:.c=.o)

# Source files for second target (assuming different main file)
SRC2 = dberror.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c test_assign2_2.c
OBJ2 = $(SRC2:.c=.o)

# Default target
all: $(TARGET) $(TARGET2)

# Original target build rule
$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^

# New target build rule
$(TARGET2): $(OBJ2)
	$(CC) $(CFLAGS) -o $@ $^

# Pattern rule for object files
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(TARGET) $(TARGET2) $(OBJ) $(OBJ2)

run: $(TARGET)
	./$(TARGET)

run2: $(TARGET2)  # New run command for second target
	./$(TARGET2)