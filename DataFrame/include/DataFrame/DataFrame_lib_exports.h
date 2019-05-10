//
// Created by mgooch on 5/9/2019.
//

#pragma once

#ifndef HMDF_DLL_EXPORTS_H
#define HMDF_DLL_EXPORTS_H


#ifdef HMDF_DLL_EXPORTING
#pragma message("HMDF_DLL_EXPORTING")
#if defined(_MSC_VER)
//  Microsoft
    #define HMDF_DLL_API __declspec(dllexport)
#elif defined(__GNUC__)
//  GCC
    #define HMDF_DLL_API __attribute__((visibility("default")))
#else
//  do nothing and hope for the best?
    #define HMDF_DLL_API
    #pragma warning Unknown dynamic link import/export semantics.
#endif
#else
#pragma message("HMDF_DLL_IMPORTING")
#if defined(_MSC_VER)
//  Microsoft
    #define HMDF_DLL_API __declspec(dllimport)
#elif defined(__GNUC__)
//  GCC
    #define HMDF_DLL_API
#else
//  do nothing and hope for the best?
    #define HMDF_DLL_API
    #pragma warning Unknown dynamic link import/export semantics.
#endif
#endif //HMDF_DLL_EXPORTING

#endif //HMDF_DLL_EXPORTS_H
