//
// Created by mgooch on 5/9/2019.
//

#pragma once
#ifndef HMDF_DLL_EXPORTS_H
#define HMDF_DLL_EXPORTS_H

#ifdef DATAFRAME_SHARED
    #ifdef HMDF_DATAFRAME_DLL_EXPORTING
        #pragma message("HMDF_DLL_EXPORTING")
        #if defined(_MSC_VER)
            //  Microsoft
            #define HMDF_DLL_API __declspec( dllexport )
        #elif defined(__GNUC__)
            //  GCC
            #define HMDF_DLL_API __attribute__((visibility("default")))
        #else
            //  do nothing and hope for the best?
            #define HMDF_DLL_API
            #pragma warning Unknown dynamic link import/export semantics.
        #endif //OS_CHECKS
    #else
        #pragma message("HMDF_DLL_IMPORTING")
        #if defined(_MSC_VER)
            //  Microsoft
            #define HMDF_DLL_API __declspec( dllimport )
        #elif defined(__GNUC__)
            //  GCC
            #define HMDF_DLL_API
        #else
            //  do nothing and hope for the best?
            #define HMDF_DLL_API
            #pragma warning Unknown dynamic link import/export semantics.
        #endif //OS_CHECKS
    #endif //HMDF_DATAFRAME_DLL_EXPORTING
#else
    #pragma message("NOT_USING_DLL_MODE")
    #define HMDF_DLL_API
#endif //DATAFRAME_SHARED

#endif//HMDF_DLL_EXPORTS_H
