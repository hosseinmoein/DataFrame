#pragma once

#ifdef HMDF_SHARED
#  ifdef _WIN32
#    ifdef HMDF_EXPORTS
#      define HMDF_API __declspec(dllexport)
#    else
#      define HMDF_API __declspec(dllimport)
#    endif // HMDF_EXPORTS
#  else
#    define HMDF_API __attribute__((visibility("default")))
#  endif // _WIN32
#else
#  define HMDF_API
#endif // HMDF_SHARED
