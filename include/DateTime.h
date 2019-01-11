// Hossein Moein
// March 18, 2018
// Copyright (C) 2018-2019 Hossein Moein
// Distributed under the BSD Software License (see file License)

#pragma once

#ifdef _WIN32
#include <windows.h>
#endif // _WIN32

#include <cstdio>
#include <ctime>
#include <stdexcept>

#include <sys/timeb.h>

#include <DMScu_FixedSizeString.h>

// ----------------------------------------------------------------------------

namespace hmdf
{

// The following constants are for formatting values in
// DateTime::string_format().
//
enum class DT_FORMAT : unsigned short int  {
    AMR_DT = 1,        // e.g. 09/16/99
    AMR_DT_CTY = 2,    // e.g. 09/16/1999
    EUR_DT = 3,        // e.g. 16/09/99
    EUR_DT_CTY = 4,    // e.g. 16/09/1999
    DT_TM = 5,         // e.g. 09/16/1999 13:51:04
    SCT_DT = 6,        // e.g. Sep 16, 1999
    DT_MMDDYYYY = 7,   // e.g. 09161999
    DT_YYYYMMDD = 8,   // e.g. 19990916
    DLR_MNY = 9,       // e.g. $ 120350045
    DLR_MNY_C = 10,    // e.g. $ 120,350,045
    DLR_MNY_C_DM = 11, // e.g. $ 120,350,045.53
    VAL_32ND = 12,     // e.g. 105-164
    VAL_64TH = 13,     // e.g. 105=33
    DT_TM2 = 14,       // e.g. 09/16/1999 13:51:04.256
    DT_DATETIME = 15,  // e.g. 20010103   09:31:15.124
    DT_FAME_DATE = 16  // e.g. 27Sep2001
};

// DO NOT change the values of these enums. They are offsets to an
// static array.
//
enum class DT_TIME_ZONE : short int  {
    LOCAL = -2,
    GMT = 0,
    AM_BUENOS_AIRES = 1,
    AM_CHICAGO = 2,
    AM_LOS_ANGELES = 3,
    AM_MEXICO_CITY = 4,
    AM_NEW_YORK = 5,
    AS_DUBAI = 6,
    AS_HONG_KONG = 7,
    AS_SHANGHAI = 8,
    AS_SINGAPORE = 9,
    AS_TEHRAN = 10,
    AS_TEL_AVIV = 11,
    AS_TOKYO = 12,
    AU_MELBOURNE = 13,
    AU_SYDNEY = 14,
    BR_RIO_DE_JANEIRO = 15,
    EU_BERLIN = 16,
    EU_LONDON = 17,
    EU_MOSCOW = 18,
    EU_PARIS = 19,
    EU_ROME = 20,
    EU_VIENNA = 21,
    EU_ZURICH = 22,
    UTC = 23,
    AS_SEOUL = 24,
    AS_TAIPEI = 25,
    EU_STOCKHOLM = 26,
    NZ = 27,
    EU_OSLO = 28,
    EU_WARSAW = 29,
    EU_BUDAPEST = 30
};

// 1 - 7 (Sunday - Saturday)
// DO NOT change the numbers
//
enum class DT_WEEKDAY : unsigned char  {
    BAD_DAY = 0,
    SUN = 1,
    MON = 2,
    TUE = 3,
    WED = 4,
    THU = 5,
    FRI = 6,
    SAT = 7
};

// 1 - 12 (January - December)
// DO NOT change the numbers
//
enum class DT_MONTH : unsigned char  {
    BAD_MONTH = 0,
    JAN = 1,
    FEB = 2,
    MAR = 3,
    APR = 4,
    MAY = 5,
    JUN = 6,
    JUL = 7,
    AUG = 8,
    SEP = 9,
    OCT = 10,
    NOV = 11,
    DEC = 12
};

// AME_STYLE: MM/DD/YYYY
// EUR_STYLE: YYYY/MM/DD
//
enum class DT_DATE_STYLE : unsigned char  {
    YYYYMMDD = 1,
    AME_STYLE = 2,
    EUR_STYLE = 3
};

// ----------------------------------------------------------------------------

class   DateTime  {

 private:

    static const char   *TIMEZONES_[];

public:

    // NOTE: This method is not multithread-safe. This method
    //       modifies the TZ environment variable which changes the
    //       time zone for the entire program.
    //
    inline void set_timezone (DT_TIME_ZONE tz)  {

        const EpochType t = time ();

        time_zone_ = tz;
        breaktime_ (t, msec ());
        return;
    }
    inline DT_TIME_ZONE get_timezone () const  { return (time_zone_); }


    using DateType = unsigned int;           // e.g. 20190110
    using DatePartType = unsigned short int; // e.g. year, month etc.
    using HourType = unsigned short int;     // 1 - 3,600
    using MinuteType = unsigned short int;   // 1 - 60
    using SecondType = unsigned short int;   // 1 - 60
    using MillisecondType = short int;       // 1 - 1,000
    using MicrosecondType = int;             // 1 - 1,000,000
    using NanosecondType =  int;             // 1 - 1,000,000,000
    using EpochType = time_t;

private:

    static const EpochType  INVALID_TIME_T_ = -1;

public:

    explicit DateTime (DT_TIME_ZONE the_zone = DT_TIME_ZONE::LOCAL) noexcept;
    explicit DateTime (DateType d,
                       HourType hr = 0,
                       MinuteType mn = 0,
                       SecondType sc = 0,
                       MillisecondType ms = 0,
                       DT_TIME_ZONE ttype = DT_TIME_ZONE::LOCAL) noexcept;
    DateTime (const DateTime &that) = default;
    DateTime (DateTime &&that) = default;

    ~DateTime () = default;

    explicit DateTime (const char *s,
                       DT_DATE_STYLE ds = DT_DATE_STYLE::YYYYMMDD,
                       DT_TIME_ZONE tz = DT_TIME_ZONE::LOCAL);

    void set_time (EpochType the_time, MillisecondType millis = 0) noexcept;

    DateTime &operator = (const DateTime &rhs) = default;

    int dt_compare(const DateTime &rhs) const;

public:

    inline DateType date () const noexcept  {        // eg. 20020303

        if (date_ == DateType (INVALID_TIME_T_))
            const_cast<DateTime *>(this)->breaktime_ (time (), msec ());

        return (date_);
    }
    inline DatePartType year () const noexcept  {    // eg. 1990

        return (date () / 10000);
    }
    inline DT_MONTH month () const noexcept  {          // JAN - DEC

        return (static_cast<DT_MONTH>((date () % 10000) / 100));
    }
    inline DatePartType dmonth () const noexcept  {  // 1 - 31

         return (date () % 100);
    }
    inline DatePartType dyear () const noexcept  {   // 1 - 366

        struct  tm  ltime;

        // It _always_ makes me sad to use const_cast<>. But then I get
        // over it.
        //
        const_cast<DateTime *>(this)->time_ = maketime_ (ltime);

        return (ltime.tm_yday + 1);
    }
    inline DT_WEEKDAY dweek () const noexcept  {   // SUN - SAT

        // It _always_ makes me sad to use const_cast<>. But then I get
        // over it.
        //
        if (week_day_ == DT_WEEKDAY::BAD_DAY)
            const_cast<DateTime *>(this)->breaktime_ (time (), msec ());

        return (week_day_);
    }
    inline HourType hour () const noexcept  {    // 0 - 23

        // It _always_ makes me sad to use const_cast<>. But then I get
        // over it.
        //
        if (hour_ == HourType (INVALID_TIME_T_))
            const_cast<DateTime *>(this)->breaktime_ (time (), msec ());

        return (hour_);
    }
    inline MinuteType minute () const noexcept  {  // 0 - 59

        if (minute_ == MinuteType (INVALID_TIME_T_))
            const_cast<DateTime *>(this)->breaktime_ (time (), msec ());

        return (minute_);
    }
    inline SecondType sec () const noexcept  {     // 0 - 59

        if (second_ == SecondType (INVALID_TIME_T_))
            const_cast<DateTime *>(this)->breaktime_ (time (), msec ());

        return (second_);
    }
    inline MillisecondType msec () const noexcept  {    // 0 - 999

        const   double  slug = double (nanosec ()) / 1000000.0 + 0.5;

        return(static_cast<MillisecondType>(slug < 1000.0 ? slug : 999.0));
    }
    inline MicrosecondType microsec () const noexcept  { // 0 - 999,999

        const   double  slug = double (nanosec ()) / 1000.0 + 0.5;

        return (static_cast<MicrosecondType>
                    (slug < 1000000.0 ? slug : 999999.0));
    }
    inline NanosecondType nanosec () const noexcept  {  // 0 - 999,999,999

        return (nanosecond_);
    }

    // The reason for time_ being INVALID_TIME_T_ is performance. sometimes
    // time_ has to be calculated by mktime() which is very expensive. In
    // those cases we assign INVALID_TIME_T_ to time_ and whenever, it
    // becomes necessary to calculate time_ (i.e. below), we do it only
    // then.
    //
    inline EpochType time () const noexcept  { // Like time()

        if (time_ == EpochType (INVALID_TIME_T_))  {
            struct tm   ltime;

            // It _always_ makes me sad to use const_cast<>. But then I get
            // over it.
            //
            const_cast<DateTime *>(this)->time_ = maketime_ (ltime);
        }

        return (time_);
    }

    DatePartType days_in_month () const noexcept;

    inline double diff_seconds (const DateTime &that) const  {

        // Currently I don't have time to implement this. There are
        // more important things to do. But we should do it at some
        // point.
        //
        if (time_zone_ != that.time_zone_)
            throw std::runtime_error ("DateTime::diff_seconds(): "
                                      "Time difference "
                                      "between different time zones "
                                      "is not implemented currently.");

        const double    this_time = static_cast<double>(time ()) +
                                (static_cast<double>(msec ()) / 1000.0);
        const double    that_time = static_cast<double>(that.time ()) +
                                (static_cast<double>(that.msec ()) / 1000.0);

        return (this_time - that_time);
    }
    inline double diff_minutes (const DateTime &that) const noexcept  {

        return (diff_seconds (that) / 60.0);
    }
    inline double diff_hours (const DateTime &that) const noexcept  {

        return (diff_minutes (that) / 60.0);
    }
    inline double diff_days (const DateTime &that) const noexcept  {

        return (diff_hours (that) / 24.0);
    }

    inline double diff_weekdays (const DateTime &that) const noexcept  {

        const int   addend = dt_compare(that) ? -1 : 1;
        DateTime    slug (that);
        double      ddays = 0.0;

        while (slug.date () != date ())  {
            if (! slug.is_weekend ())
                ddays += static_cast<const double>(addend);
            slug.add_days (addend);
        }

        return (ddays + (diff_seconds (slug) /
                         static_cast<double>(24 * 3600)));
    }
    inline double diff_weeks (const DateTime &that) const noexcept  {

        return (diff_days (that) / 7.0);
    }

    inline void add_seconds (EpochType secs) noexcept  {

        set_time (time () + secs, msec ());
        return;
    }
    inline void add_days (long days) noexcept  {

        if (days != 0)  {
            const int   addend = days < 0 ? -1 : 1;

            while (days)  {
                const DateTime  prev_date (*this);

                add_seconds (addend * 24 * 3600);

                // Take care of DST vs EST:
                // Ask me about why I have while loops
                //
                if (addend > 0)  {
                    if (date () == prev_date.date ())
                        while (date () == prev_date.date ())
                            add_seconds (3600);
                    else if (int(dyear ()) - int(prev_date.dyear ()) > 1)
                        while (int(dyear ()) - int(prev_date.dyear ()) > 1)
                            add_seconds (-3600);
                    else if (hour () < prev_date.hour ())
                        while (hour () < prev_date.hour ())
                            add_seconds (3600);
                    else if (hour () > prev_date.hour ())
                        while (hour () > prev_date.hour ())
                            add_seconds (-3600);
                }
                else  {
                    if (date () == prev_date.date ())
                        while (date () == prev_date.date ())
                            add_seconds (-3600);
                    else if (int(dyear ()) - int(prev_date.dyear ()) < -1)
                        while (int(dyear ()) - int(prev_date.dyear()) < -1)
                            add_seconds (3600);
                    else if (hour () < prev_date.hour ())
                        while (hour () < prev_date.hour ())
                            add_seconds (3600);
                    else if (hour () > prev_date.hour ())
                        while (hour () > prev_date.hour ())
                            add_seconds (-3600);
                }

                days -= addend;
            }
        }

        return;
    }

    inline bool is_weekend () const noexcept  {

        const DT_WEEKDAY    w_day = dweek ();

        return (w_day == DT_WEEKDAY::SUN || w_day == DT_WEEKDAY::SAT);
    }

    inline bool is_newyear () const noexcept  {

        const DatePartType  m_day = dmonth (); // 1 - 31
        const DT_MONTH      mon = month ();    // JAN - DEC
        const DT_WEEKDAY    w_day = dweek ();  // SUN - SAT

        // New Year's Day (January 1 or Monday, January 2 or Friday,
        // December 31)
        //
        return (((m_day == 1 || (w_day == DT_WEEKDAY::MON && m_day == 2)) &&
                 mon == DT_MONTH::JAN) ||
                (m_day == 31 && w_day == DT_WEEKDAY::FRI &&
                 mon == DT_MONTH::DEC));
    }

    inline bool is_xmas () const noexcept  {

        const DatePartType  m_day = dmonth (); // 1 - 31
        const DT_MONTH      mon = month ();    // JAN - DEC
        const DT_WEEKDAY    w_day = dweek ();  // SUN - SAT

        // Christmas Day (December 25 or Monday, December 26 or Friday,
        // December 24)
        //
        return ((m_day == 25 || (w_day == DT_WEEKDAY::MON && m_day == 26) ||
                 (w_day == DT_WEEKDAY::FRI && m_day == 24)) &&
                mon == DT_MONTH::DEC);
    }

    bool is_us_business_day () const noexcept;
    bool is_us_bank_holiday () const noexcept;
    bool is_valid () const noexcept;

    inline void add_weekdays (long days) noexcept  {

        if (days != 0)  {
            const int   addend = days < 0 ? -1 : 1;

            while (days)  {
                add_days (addend);
                // while (is_weekend () || is_newyear () || is_xmas ())
                while (is_weekend ())
                    add_days (addend);
                days -= addend;
            }
        }
        return;
    }

    DateTime &operator = (DateType rhs);

    // Currently, the following formats are supported:
    //  1)  YYYYMMDD [LOCAL | GMT]
    //  2)  YYYYMMDD HH:MM:SS.MMM [LOCAL | GMT]
    //
    DateTime &operator = (const char *rhs);

    template<typename T>
    inline void date_to_str (DT_FORMAT format, T &result) const;

    inline std::string string_format (DT_FORMAT format) const  {

        std::string  result;

        date_to_str (format, result);
        return (result);
    }

 private:

    // This guy initializes anything that needs to be initialized
    // statically.
    //
    class   DI_initializer  { public: DI_initializer () noexcept; };

    static const DI_initializer di_init_;

    friend class    DI_initializer;

protected:

    DateType        date_ { };      // e.g. 20001025
    HourType        hour_ { };
    MinuteType      minute_ { };
    SecondType      second_ { };
    NanosecondType  nanosecond_ { };
    EpochType       time_ { };      // Seconds since 01/01/1970 (Epoch)
    DT_WEEKDAY      week_day_ { };
    DT_TIME_ZONE    time_zone_ { };

    // NOTE: This method is not multithread-safe. This method
    //       modifies the TZ environment variable which changes the
    //       time zone for the entire program.
    //
    inline EpochType maketime_ (struct tm &ltime) const noexcept  {

        ltime.tm_sec = sec ();
        ltime.tm_isdst = -1;
        ltime.tm_min = minute ();
        ltime.tm_hour = hour ();
        ltime.tm_mday = dmonth ();
        ltime.tm_mon = static_cast<int>(month()) - 1;
        ltime.tm_year = year () - 1900;

        if (time_zone_ != DT_TIME_ZONE::LOCAL)  {
#ifdef _WIN32
            // SetEnvironmentVariable (L"TZ", TIMEZONES_ [time_zone_]);
            putenv (TIMEZONES_[static_cast<int>(time_zone_)]);
            _tzset ();
#else
            ::setenv ("TZ", TIMEZONES_[static_cast<int>(time_zone_)], 1);
            ::tzset ();
#endif // _WIN32
        }

        const time_t    t  = ::mktime (&ltime);

        if (time_zone_ != DT_TIME_ZONE::LOCAL)  {
#ifdef _WIN32
            // SetEnvironmentVariable (L"TZ", nullptr);
            putenv ("TZ=");
            _tzset ();
#else
            ::unsetenv ("TZ");
            ::tzset ();
#endif // _WIN32
        }

        return (t);
    }

    // NOTE: This method is not multithread-safe. This method
    //       modifies the TZ environment variable which changes the
    //       time zone for the entire program.
    inline void
    //
    breaktime_ (EpochType the_time, MillisecondType millis) noexcept  {

        if (time_zone_ != DT_TIME_ZONE::LOCAL)  {
#ifdef _WIN32
            // SetEnvironmentVariable (L"TZ", TIMEZONES_ [time_zone_]);
            putenv (TIMEZONES_[static_cast<int>(time_zone_)]);
            _tzset ();
#else
            ::setenv ("TZ", TIMEZONES_[static_cast<int>(time_zone_)], 1);
            ::tzset ();
#endif // _WIN32
        }

        struct tm   ltime;

#ifdef _WIN32
        localtime_s (&ltime, &the_time);
#else
        localtime_r (&the_time, &ltime);
#endif // _WIN32

        if (time_zone_ != DT_TIME_ZONE::LOCAL)  {
#ifdef _WIN32
            // SetEnvironmentVariable (L"TZ", nullptr);
            putenv ("TZ=");
            _tzset ();
#else
            ::unsetenv ("TZ");
            ::tzset ();
#endif // _WIN32
        }

        date_ = (ltime.tm_year + 1900) * 100;

        date_ += (ltime.tm_mon + 1);
        date_ *= 100;
        date_ += ltime.tm_mday;

        hour_ = ltime.tm_hour;
        minute_ = ltime.tm_min;
        second_ = ltime.tm_sec;
        nanosecond_ = millis * 1000000;
        time_ = the_time;
        week_day_ = static_cast<DT_WEEKDAY>(ltime.tm_wday + 1);

        return;
    }

    static const char *const    MONTH_[];
    static const char *const    MONTH_BR_[];
    static const char *const    WDAY_[];
    static const char *const    WDAY_BR_[];
};

// ----------------------------------------------------------------------------

template<typename T>
inline void DateTime::
date_to_str (DT_FORMAT format, T &result) const  {

    DMScu_FixedSizeString<63>   buffer;

    switch (format)  {
        case DT_FORMAT::AMR_DT:
        {
            buffer.printf ("%002d/%002d/%002d",
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()) % 100);
        } break;

        case DT_FORMAT::AMR_DT_CTY:
        {
            buffer.printf ("%002d/%002d/%d",
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()));
        } break;

        case DT_FORMAT::EUR_DT:
        {
            buffer.printf ("%002d/%002d/%002d",
                           static_cast<int>(dmonth ()),
                           static_cast<int>(month ()),
                           static_cast<int>(year ()) % 100);
        } break;

        case DT_FORMAT::EUR_DT_CTY:
        {
            buffer.printf ("%002d/%002d/%d",
                           static_cast<int>(dmonth ()),
                           static_cast<int>(month ()),
                           static_cast<int>(year ()));
        } break;

        case DT_FORMAT::DT_TM:
        {
            buffer.printf ("%002d/%002d/%d %002d:%002d:%002d",
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()),
                           static_cast<int>(hour ()),
                           static_cast<int>(minute ()),
                           static_cast<int>(sec ()));
        } break;

        case DT_FORMAT::DT_TM2:
        {
            buffer.printf ("%002d/%002d/%d %002d:%002d:%002d.%0003d",
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()),
                           static_cast<int>(hour ()),
                           static_cast<int>(minute ()),
                           static_cast<int>(sec ()),
                           static_cast<int>(msec ()));
        } break;

        case DT_FORMAT::DT_DATETIME:
        {
            buffer.printf ("%d%002d%002d  %002d:%002d:%002d.%0003d",
                           static_cast<int>(year ()),
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(hour ()),
                           static_cast<int>(minute ()),
                           static_cast<int>(sec ()),
                           static_cast<int>(msec ()));
        } break;

        case DT_FORMAT::SCT_DT:
        {
            buffer.printf ("%s %002d, %d",
                           MONTH_BR_ [static_cast<int>(month ()) - 1],
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()));
        } break;

        case DT_FORMAT::DT_MMDDYYYY:
        {
            buffer.printf ("%002d%002d%d",
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(year ()));
        } break;

        case DT_FORMAT::DT_YYYYMMDD:
        {
            buffer.printf ("%002d%002d%002d",
                           static_cast<int>(year ()),
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()));
        } break;

        case DT_FORMAT::DT_FAME_DATE:  // e.g. 27Sep2001
        {
            buffer.printf ("%d%s%d",
                           static_cast<const int>(dmonth ()),
                           MONTH_BR_ [static_cast<int>(month ()) - 1],
                           static_cast<const int>(year ()));
        } break;

        default:
        {
            DMScu_FixedSizeString<1023>  err;

            err.printf ("ERROR: DateTime::date_to_str(): Unknown format: '%u'",
                        format);

            throw std::runtime_error (err.c_str ());
        }
    }

    result = buffer.c_str ();
    return;
}

// ----------------------------------------------------------------------------

inline bool operator == (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.dt_compare (rhs) == 0);
}

// ----------------------------------------------------------------------------

inline bool operator != (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.dt_compare (rhs) != 0);
}

// ----------------------------------------------------------------------------

inline bool operator < (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.dt_compare (rhs) < 0);
}

// ----------------------------------------------------------------------------

inline bool operator <= (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.dt_compare (rhs) <= 0);
}

// ----------------------------------------------------------------------------

inline bool operator > (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.dt_compare (rhs) > 0);
}

// ----------------------------------------------------------------------------

inline bool operator >= (const DateTime &lhs, const DateTime &rhs) noexcept  {

    return (lhs.dt_compare (rhs) >= 0);
}

// ----------------------------------------------------------------------------

template<typename S>
inline S &operator << (S &o, const DateTime &rhs)  {

    return (o << rhs.string_format (DT_FORMAT::DT_TM2));
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:

