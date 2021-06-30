# Preprocessing best-practice

## `main()`

- I often have different samples for a particular dataset, and I want to be able
  to select which one I process. To do this, I pass a filepath to the input file
  to the program. The next question is then: how to handle output paths? One
  option is to infer the output path from the input path, a more flexible option
  is to also provide an output path.

- Makefiles can't interact with remote file storages like S3, which, given that
  most of the data I work with never sits on my machine, is a major problem. Two
  possible ways forward: use makefiles with phony targets or use an alternative.


