CC = gcc
CFLAGS = -Wall -g
TARGET = test_assign1
SRC = dberror.c storage_mgr.c test_assign1_1.c
OBJ = $(SRC:.c=.o)

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(TARGET) $(OBJ)

run: ./$(TARGET)
	./$(TARGET)