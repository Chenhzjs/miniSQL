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

# Utility rule file for ExperimentalUpdate.

# Include any custom commands dependencies for this target.
include glog-build/CMakeFiles/ExperimentalUpdate.dir/compiler_depend.make

# Include the progress variables for this target.
include glog-build/CMakeFiles/ExperimentalUpdate.dir/progress.make

glog-build/CMakeFiles/ExperimentalUpdate:
	cd /mnt/d/miniSQL/cmake-build-debug-wsl/glog-build && /usr/bin/ctest -D ExperimentalUpdate

ExperimentalUpdate: glog-build/CMakeFiles/ExperimentalUpdate
ExperimentalUpdate: glog-build/CMakeFiles/ExperimentalUpdate.dir/build.make
.PHONY : ExperimentalUpdate

# Rule to build all files generated by this target.
glog-build/CMakeFiles/ExperimentalUpdate.dir/build: ExperimentalUpdate
.PHONY : glog-build/CMakeFiles/ExperimentalUpdate.dir/build

glog-build/CMakeFiles/ExperimentalUpdate.dir/clean:
	cd /mnt/d/miniSQL/cmake-build-debug-wsl/glog-build && $(CMAKE_COMMAND) -P CMakeFiles/ExperimentalUpdate.dir/cmake_clean.cmake
.PHONY : glog-build/CMakeFiles/ExperimentalUpdate.dir/clean

glog-build/CMakeFiles/ExperimentalUpdate.dir/depend:
	cd /mnt/d/miniSQL/cmake-build-debug-wsl && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /mnt/d/miniSQL /mnt/d/miniSQL/thirdparty/glog /mnt/d/miniSQL/cmake-build-debug-wsl /mnt/d/miniSQL/cmake-build-debug-wsl/glog-build /mnt/d/miniSQL/cmake-build-debug-wsl/glog-build/CMakeFiles/ExperimentalUpdate.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : glog-build/CMakeFiles/ExperimentalUpdate.dir/depend

