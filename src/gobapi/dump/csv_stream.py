class CSVStream():
    """
    A class to stream data in 'faked' multiple files

    Example:
        - file:
        a,b,c
        1,2,3
        4,5,6
        7,8,9
        - with max_read = 2 will behave like two files:
        a,b,c
        1,2,3
        4,5,6
        - and:
        a,b,c
        7,8,9

    """
    DEFAULT_READ_SIZE = 8192  # Default number of bytes to return by the read method

    def __init__(self, lines, max_read):
        """
        Initialize a CSVStream. The stream behaves like a series of individual files

        :param lines: Iterator for csv lines, first line is a header
        :param max_read: Max number of lines to read before faking end-of-file
        """
        self.lines = lines
        self.max_read = max_read

        self.buffer = ""                    # Buffer all input that has not been 'written out'
        self.count = 0                      # Count the number of lines for a faked file
        self.total_count = 0                # Count the total number of lines for all faked file reads
        self.last_line = next(lines, None)  # Extract the header
        self.header = self.last_line        # Keep this line for any subsequent reset_count's

    def reset_count(self):
        """
        Pretend that a new file is read.

        :return: None
        """
        self.count = 0

    def has_items(self):
        """
        Tells if the stream has any items to be read

        As long as nothing has been read or the last read line was not None, the answer is positive.
        :return: True if any items are expected to be available to be read
        """
        return self.last_line is not None

    def read(self, size=DEFAULT_READ_SIZE):
        """
        Read data until the requested size has been reached or the maximum number of lines has been reached

        :param size:
        :return:
        """
        if self.count == 0:
            # Start a new 'file' so start with a header and any remaining buffer contents from any previous reads
            self.buffer = self.header + self.buffer

        while len(self.buffer) < size and self.count < self.max_read:
            # Read a line, stop if:
            # - no more lines can be read
            # - the requested size has been reached
            # - the maximum number of lines has been read
            self.last_line = next(self.lines, None)
            if self.last_line is None:
                break
            # Store the line in the buffer and increment counters
            self.buffer += self.last_line
            self.count += 1
            self.total_count += 1
        # Return the requested size (or less if no more data is available)
        result = self.buffer[:size]
        # Keep any remaining data for the next read
        self.buffer = self.buffer[size:]
        return result

    def readline(self, *args, **kwargs):
        """
        This method is implemented because it is a required interface for cursor.import methods

        However, the interface is not used so it has not been implemented
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError
