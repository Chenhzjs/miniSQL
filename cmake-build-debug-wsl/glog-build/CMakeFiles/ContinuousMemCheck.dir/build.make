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

# Utility rule file for ContinuousMemCheck.

# Include any custom commands dependencies for this target.
include glog-build/CMakeFiles/ContinuousMemCheck.dir/compiler_depend.make

# Include the progress variables for this target.
include glog-build/CMakeFiles/ContinuousMemCheck.dir/progress.make

glog-build/CMakeFiles/ContinuousMemCheck:
	cd /mnt/d/miniSQL/cmake-build-debug-wsl/glog-build && /usr/bin/ctest -D ContinuousMemCheck

ContinuousMemCheck: glog-build/CMakeFiles/ContinuousMemCheck
ContinuousMemCheck: glog-build/CMakeFiles/ContinuousMemCheck.dir/build.make
.PHONY : ContinuousMemCheck

# Rule to build all files generated by this target.
glog-build/CMakeFiles/ContinuousMemCheck.dir/build: ContinuousMemCheck
.PHONY : glog-build/CMakeFiles/ContinuousMemCheck.dir/build

glog-build/CMakeFiles/ContinuousMemCheck.dir/clean:
	cd /mnt/d/miniSQL/cmake-build-debug-wsl/glog-build && $(CMAKE_COMMAND) -P CMakeFiles/ContinuousMemCheck.dir/cmake_clean.cmake
.PHONY : glog-build/CMakeFiles/ContinuousMemCheck.dir/clean

glog-build/CMakeFiles/ContinuousMemCheck.dir/depend:
	cd /mnt/d/miniSQL/cmake-build-debug-wsl && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /mnt/d/miniSQL /mnt/d/miniSQL/thirdparty/glog /mnt/d/miniSQL/cmake-build-debug-wsl /mnt/d/miniSQL/cmake-build-debug-wsl/glog-build /mnt/d/miniSQL/cmake-build-debug-wsl/glog-build/CMakeFiles/ContinuousMemCheck.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : glog-build/CMakeFiles/ContinuousMemCheck.dir/depend

