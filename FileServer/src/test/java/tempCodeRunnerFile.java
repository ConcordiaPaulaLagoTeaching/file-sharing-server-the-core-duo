    @Test
    void testWriteAndReadFile() throws Exception {
        fs.createFile("a.txt");
        fs.writeFile("a.txt", "hello".getBytes());
        assertEquals("hello", new String(fs.readFile("a.txt")));
    }
