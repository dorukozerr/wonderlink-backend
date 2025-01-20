const test = async () => {
  let objects: { data: string }[] = [];
  for (let i = 0; i < 1000000; i++) {
    objects.push({ data: 'test' });
  }

  const beforeMem = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`Memory usage before GC: ${beforeMem.toFixed(2)} MB`);

  objects = [];

  if (gc) gc();

  const afterMem = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`Memory usage after GC: ${afterMem.toFixed(2)} MB`);
};

test();
