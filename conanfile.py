from conans import ConanFile, CMake

class DataFrameConan(ConanFile):
    name = "Dataframe"
    version = "1.0.0"
    license = "BSD 3-Clause"
    url = "https://github.com/hosseinmoein/DataFrame"
    description = "R's and Pandas DataFrame in modern C++"
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False]}
    default_options = {"shared": False}
    generators = "cmake"
    exports_sources = "*"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        cmake.test()

    def package(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["Dataframe"]
