CC = gcc
CFLAGS = -Wall -g
TARGET = test_assign2_1
SRC = dberror.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c test_assign2_1.c
OBJ = $(SRC:.c=.o)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(TARGET) $(OBJ)

run: ./$(TARGET)
	./$(TARGET)