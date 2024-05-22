# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.22

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /mnt/d/miniSQL

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /mnt/d/miniSQL/cmake-build-debug-wsl

# Include any dependencies generated for this target.
include test/CMakeFiles/minisql_test_main.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include test/CMakeFiles/minisql_test_main.dir/compiler_depend.make

# Include the progress variables for this target.
include test/CMakeFiles/minisql_test_main.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/minisql_test_main.dir/flags.make

test/CMakeFiles/minisql_test_main.dir/main_test.cpp.o: test/CMakeFiles/minisql_test_main.dir/flags.make
test/CMakeFiles/minisql_test_main.dir/main_test.cpp.o: ../test/main_test.cpp
test/CMakeFiles/minisql_test_main.dir/main_test.cpp.o: test/CMakeFiles/minisql_test_main.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/mnt/d/miniSQL/cmake-build-debug-wsl/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/minisql_test_main.dir/main_test.cpp.o"
	cd /mnt/d/miniSQL/cmake-build-debug-wsl/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT test/CMakeFiles/minisql_test_main.dir/main_test.cpp.o -MF CMakeFiles/minisql_test_main.dir/main_test.cpp.o.d -o CMakeFiles/minisql_test_main.dir/main_test.cpp.o -c /mnt/d/miniSQL/test/main_test.cpp

test/CMakeFiles/minisql_test_main.dir/main_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/minisql_test_main.dir/main_test.cpp.i"
	cd /mnt/d/miniSQL/cmake-build-debug-wsl/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /mnt/d/miniSQL/test/main_test.cpp > CMakeFiles/minisql_test_main.dir/main_test.cpp.i

test/CMakeFiles/minisql_test_main.dir/main_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/minisql_test_main.dir/main_test.cpp.s"
	cd /mnt/d/miniSQL/cmake-build-debug-wsl/test && /usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /mnt/d/miniSQL/test/main_test.cpp -o CMakeFiles/minisql_test_main.dir/main_test.cpp.s

# Object files for target minisql_test_main
minisql_test_main_OBJECTS = \
"CMakeFiles/minisql_test_main.dir/main_test.cpp.o"

# External object files for target minisql_test_main
minisql_test_main_EXTERNAL_OBJECTS =

test/libminisql_test_main.so: test/CMakeFiles/minisql_test_main.dir/main_test.cpp.o
test/libminisql_test_main.so: test/CMakeFiles/minisql_test_main.dir/build.make
test/libminisql_test_main.so: glog-build/libglogd.so.0.6.0
test/libminisql_test_main.so: lib/libgtestd.so.1.11.0
test/libminisql_test_main.so: test/CMakeFiles/minisql_test_main.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/mnt/d/miniSQL/cmake-build-debug-wsl/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX shared library libminisql_test_main.so"
	cd /mnt/d/miniSQL/cmake-build-debug-wsl/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/minisql_test_main.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/minisql_test_main.dir/build: test/libminisql_test_main.so
.PHONY : test/CMakeFiles/minisql_test_main.dir/build

test/CMakeFiles/minisql_test_main.dir/clean:
	cd /mnt/d/miniSQL/cmake-build-debug-wsl/test && $(CMAKE_COMMAND) -P CMakeFiles/minisql_test_main.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/minisql_test_main.dir/clean

test/CMakeFiles/minisql_test_main.dir/depend:
	cd /mnt/d/miniSQL/cmake-build-debug-wsl && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /mnt/d/miniSQL /mnt/d/miniSQL/test /mnt/d/miniSQL/cmake-build-debug-wsl /mnt/d/miniSQL/cmake-build-debug-wsl/test /mnt/d/miniSQL/cmake-build-debug-wsl/test/CMakeFiles/minisql_test_main.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/minisql_test_main.dir/depend

