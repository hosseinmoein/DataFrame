// Hossein Moein
// March 18, 2018
/*
Copyright (c) 2019-2022, Hossein Moein
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
* Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
* Neither the name of Hossein Moein and/or the DataFrame nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL Hossein Moein BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <DataFrame/Utils/DateTime.h>
#include <DataFrame/Utils/FixedSizeString.h>

#ifdef _MSC_VER
#  ifdef _MSC_EXTENSIONS
#    define DELTA_EPOCH_IN_MICROSECS  11644473600000000Ui64
#  else
#    define DELTA_EPOCH_IN_MICROSECS  11644473600000000ULL
#  endif // _MSC_EXTENSIONS
#else
#  include <sys/time.h>
#endif // _MSC_VER

// ----------------------------------------------------------------------------

namespace hmdf
{

DateTime::DateTime (DT_TIME_ZONE tz) : time_zone_(tz)  {

#ifdef _MSC_VER
    FILETIME            ft;
    unsigned __int64    tmpres = 0;

    GetSystemTimeAsFileTime(&ft);

    tmpres |= ft.dwHighDateTime;
    tmpres <<= 32;
    tmpres |= ft.dwLowDateTime;

    tmpres /= 10;  // convert into microseconds

    // converting file time to unix epoch
    //
    tmpres -= DELTA_EPOCH_IN_MICROSECS;

    set_time(tmpres / 1000000UL, (tmpres % 1000000UL) * 1000000);
#elif defined HMDF_HAVE_CLOCK_GETTIME
    struct timespec ts;

    ::clock_gettime(CLOCK_REALTIME, &ts);
    set_time(ts.tv_sec, ts.tv_nsec);
#else
    struct timeval  tv { };

    ::gettimeofday(&tv, nullptr);
    set_time(tv.tv_sec, tv.tv_usec * 1000);
#endif // _MSC_VER
}

// ----------------------------------------------------------------------------

void DateTime::set_time(EpochType the_time, NanosecondType nanosec) noexcept {

    date_ = INVALID_DATE_;
    hour_ = INVALID_HOUR_;
    minute_ = INVALID_MINUTE_;
    second_ = INVALID_SECOND_;
    week_day_ = DT_WEEKDAY::BAD_DAY;

    nanosecond_ = nanosec;
    time_ = the_time;
}

// ----------------------------------------------------------------------------

DateTime::DateTime (DateType d,
                    HourType hr,
                    MinuteType mn,
                    SecondType sc,
                    NanosecondType ns,
                    DT_TIME_ZONE tz)
    : date_ (d),
      hour_ (hr),
      minute_ (mn),
      second_ (sc),
      nanosecond_ (ns),
      time_zone_ (tz)  {
}

// ----------------------------------------------------------------------------

// I'm adding the following formats:
//
// AME_STYLE:
//  (1)  MM/DD/YYYY
//  (2)  MM/DD/YYYY HH
//  (3)  MM/DD/YYYY HH:MM
//  (4)  MM/DD/YYYY HH:MM:SS
//  (5)  MM/DD/YYYY HH:MM:SS.MMM
//
// EUR_STYLE:
//  (1)  YYYY/MM/DD
//  (2)  YYYY/MM/DD HH
//  (3)  YYYY/MM/DD HH:MM
//  (4)  YYYY/MM/DD HH:MM:SS
//  (5)  YYYY/MM/DD HH:MM:SS.MMM
//
// ISO_STYLE:
//  (12) YYYY-MM-DD
//  (13) YYYY-MM-DD HH
//  (14) YYYY-MM-DD HH:MM
//  (15) YYYY-MM-DD HH:MM:SS
//  (16) YYYY-MM-DD HH:MM:SS.MMM
//
DateTime::DateTime (const char *s, DT_DATE_STYLE ds, DT_TIME_ZONE tz)
    : time_zone_ (tz)  {

    const char  *str = s;

    while (::isspace (*str)) ++str;

    if (ds == DT_DATE_STYLE::YYYYMMDD)
        *this = str;
    else  {
        const size_t    str_len = ::strlen (str);
        int             year { 0 }, month { 0 }, day { 0 };

        hour_ = minute_ = second_ = nanosecond_ = 0;
        if (ds == DT_DATE_STYLE::AME_STYLE)  {
            if (str_len <= 10)  {
                ::sscanf (str, "%d/%d/%d", &month, &day, &year);
            }
            else if (str_len == 13)  {
                ::sscanf (str, "%d/%d/%d %hd", &month, &day, &year, &hour_);
            }
            else if (str_len == 16)  {
                ::sscanf (str, "%d/%d/%d %hd:%hd",
                          &month, &day, &year, &hour_, &minute_);
            }
            else if (str_len == 19)  {
                ::sscanf (str, "%d/%d/%d %hd:%hd:%hd",
                          &month, &day, &year, &hour_, &minute_, &second_);
            }
            else if (str_len == 23)  {
                MillisecondType ms { 0 };

                ::sscanf (str, "%d/%d/%d %hd:%hd:%hd.%hd",
                          &month, &day, &year, &hour_, &minute_, &second_, &ms);
                nanosecond_ = ms * 1000000;
            }
        }
        else if (ds == DT_DATE_STYLE::EUR_STYLE)  {
            if (str_len <= 10)  {
                ::sscanf (str, "%d/%d/%d", &year, &month, &day);
            }
            else if (str_len == 13)  {
                ::sscanf (str, "%d/%d/%d %hd", &year, &month, &day, &hour_);
            }
            else if (str_len == 16)  {
                ::sscanf (str, "%d/%d/%d %hd:%hd",
                          &year, &month, &day, &hour_, &minute_);
            }
            else if (str_len == 19)  {
                ::sscanf (str, "%d/%d/%d %hd:%hd:%hd",
                          &year, &month, &day, &hour_, &minute_, &second_);
            }
            else if (str_len == 23)  {
                MillisecondType ms { 0 };

                ::sscanf (str, "%d/%d/%d %hd:%hd:%hd.%hd",
                          &year, &month, &day, &hour_, &minute_, &second_, &ms);
                nanosecond_ = ms * 1000000;
            }
        }
        else if (ds == DT_DATE_STYLE::ISO_STYLE)  {
            if (str_len <= 10)  {
                ::sscanf (str, "%d-%d-%d", &year, &month, &day);
            }
            else if (str_len == 13)  {
                ::sscanf (str, "%d-%d-%d %hd", &year, &month, &day, &hour_);
            }
            else if (str_len == 16)  {
                ::sscanf (str, "%d-%d-%d %hd:%hd",
                          &year, &month, &day, &hour_, &minute_);
            }
            else if (str_len == 19)  {
                ::sscanf (str, "%d-%d-%d %hd:%hd:%hd",
                          &year, &month, &day, &hour_, &minute_, &second_);
            }
            else if (str_len == 23)  {
                MillisecondType millis { 0 };

                ::sscanf (str, "%d-%d-%d %hd:%hd:%hd.%hd",
                          &year, &month, &day, &hour_, &minute_,
                          &second_, &millis);
                nanosecond_ = millis * 1000000;
            }
            else if (str_len > 23)  {
                MicrosecondType micros { 0 };

                ::sscanf (str, "%d-%d-%d %hd:%hd:%hd.%d",
                          &year, &month, &day, &hour_, &minute_,
                          &second_, &micros);
                nanosecond_ = micros * 1000;
            }
        }
        if (year == 0 && month == 0 && day == 0)  {
            String512   err;

            err.printf ("DateTime::DateTime(const char *): Don't know "
                        "how to parse '%s'", s);
            throw std::runtime_error (err.c_str ());
        }
        date_ = year * 10000 + month * 100 + day;
    }
}

// ----------------------------------------------------------------------------

// There are many different ways of representing
// a date-time value. We will add code as need arises.
//
// Currently, the following formats are supported:
//  (1)  YYYYMMDD
//  (2)  YYYYMMDD HH
//  (3)  YYYYMMDD HH:MM
//  (4)  YYYYMMDD HH:MM:SS
//  (5)  YYYYMMDD HH:MM:SS.MMM
//
DateTime &DateTime::operator = (const char *s)  {

    time_ = INVALID_TIME_T_;

    const char  *str = s;

    while (::isspace (*str)) ++str;

    const size_t   str_len = ::strlen (str);

    if (str_len == 8)  {
        hour_ = minute_ = second_ = nanosecond_ = 0;
        ::sscanf (str, "%d", &date_);
    }
    else if (str_len == 11)  {
        minute_ = second_ = nanosecond_ = 0;
        ::sscanf (str, "%d %hd", &date_, &hour_);
    }
    else if (str_len == 14)  {
        second_ = nanosecond_ = 0;
        ::sscanf (str, "%d %hd:%hd", &date_, &hour_, &minute_);
    }
    else if (str_len == 17)  {
        nanosecond_ = 0;
        ::sscanf (str, "%d %hd:%hd:%hd", &date_, &hour_, &minute_, &second_);
    }
    else if (str_len == 21)  {
        MillisecondType ms;

        ::sscanf (str, "%d %hd:%hd:%hd.%hd",
                  &date_, &hour_, &minute_, &second_, &ms);
        nanosecond_ = ms * 1000000;;
    }
    else  {
        String512   err;

        err.printf ("DateTime::operator=(const char *): Don't know how to "
                    "parse '%s'", s);
        throw std::runtime_error (err.c_str ());
    }

    return (*this);
}

// ----------------------------------------------------------------------------

DateTime &DateTime::operator = (DateType the_date)  {

    String512   buffer;

    buffer.printf ("%u", the_date);
    *this = buffer.c_str ();
    return (*this);
}

// ----------------------------------------------------------------------------

DateTime::EpochType DateTime::compare (const DateTime &rhs) const  {

    return (long_time () - rhs.long_time ());
}

// ----------------------------------------------------------------------------

DateTime::DatePartType DateTime::days_in_month () const noexcept  {

    return (days_in_month_(month(), year()));
}

// ----------------------------------------------------------------------------

DateTime::DatePartType DateTime::
days_in_month_ (DT_MONTH month, DatePartType year) noexcept  {

    switch (month)  {
        case DT_MONTH::APR:
        case DT_MONTH::JUN:
        case DT_MONTH::SEP:
        case DT_MONTH::NOV:
            return (30);
        case DT_MONTH::FEB:
            // This I remember from CML.
            //
            if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)
                return (29);
            else
                return (28);
        default:
            return (31);
    }
}

// ----------------------------------------------------------------------------

// Notice that we are not checking time_ to be valid.
// This is potentially a bug
//
bool DateTime::is_valid () const noexcept  {

    return (year () > 0 && year () < 9999 &&
            month () > DT_MONTH::BAD_MONTH && month () <= DT_MONTH::DEC &&
            dmonth () > 0 &&
            dmonth () <= days_in_month_ (month(), year()) &&
            hour () >= 0 && hour () < 24 &&
            minute () >= 0 && minute () < 60 &&
            sec () >= 0 && sec () < 60 &&
            nanosec () >= 0 && nanosec () < 1000000000);
}

// ----------------------------------------------------------------------------

// Is Good Friday a business day?
// No, it isn't, but calculating its date is complicated.
// To add calculation of Good Friday, see
// http://en.wikipedia.org/wiki/Computus
// for calculation of Easter Sunday.
// Good Friday is the Friday before Easter Sunday.
//
bool DateTime::is_us_business_day () const noexcept  {

    const DatePartType  m_day = dmonth ();   // 1 - 31
    const DT_MONTH      mon = month ();      // JAN - DEC
    const DT_WEEKDAY    w_day = dweek ();    // SUN - SAT
    const DateType      date_part = date();  // e.g. 20190110

    return (! (is_weekend () ||
               is_newyear () ||

               // V-J Day. End of World War II (August 15-16, 1945)
               //
               (date_part == 19450815 || date_part == 19450816) ||

              // Assassination of President John F. Kennedy (November 22, 1963)
              //
               (date_part == 19631122) ||

               // President Harry S. Truman day of mourning (December 28, 1972)
               //
               (date_part == 19721228) ||

              // President Lyndon B. Johnson day of mourning (January 25, 1973)
              //
               (date_part == 19730125) ||

               // New York City black out (July 14, 1977)
               //
               (date_part == 19770714) ||

               // President Richard M. Nixon day of mourning (April 27, 1994)
               //
               (date_part == 19940427) ||

               // President Ronald Reagan day of mourning (June 11, 2004)
               //
               (date_part == 20040611) ||

               // President Gerald R. Ford day of mourning (January 2, 2007)
               //
               (date_part == 20070102) ||

              // President George H. W. Bush day of mourning (December 5, 2018)
              //
               (date_part == 20181205) ||

               // World Trade Center attack (September 11-14, 2001)
               //
               (year () == 2001 && mon == DT_MONTH::SEP && m_day >= 11 &&
                m_day <= 14) ||

               // Martin Luther King Day (third Monday of January)
               //
               (w_day == DT_WEEKDAY::MON &&
                (m_day >= 15 && m_day <= 21) && mon == DT_MONTH::JAN) ||

               // President's Day (third Monday of February)
               //
               (w_day == DT_WEEKDAY::MON &&
                (m_day >= 15 && m_day <= 21) && mon == DT_MONTH::FEB) ||

               // Memorial Day (last Monday of May)
               //
               (w_day == DT_WEEKDAY::MON && m_day >= 25 &&
                mon == DT_MONTH::MAY) ||

               // Independence Day (July 4 or Monday, July 5 or Friday, July 3)
               //
               ((m_day == 4 || (w_day == DT_WEEKDAY::MON && m_day == 5) ||
                (w_day == DT_WEEKDAY::FRI && m_day == 3)) &&
                mon == DT_MONTH::JUL) ||

               // Labor Day (first Monday of September)
               //
               (w_day == DT_WEEKDAY::MON && m_day <= 7 &&
                mon == DT_MONTH::SEP) ||

               // Thanksgiving Day (fourth Thursday of November)
               //
               (w_day == DT_WEEKDAY::THU &&
                (m_day >= 22 && m_day <= 28) && mon == DT_MONTH::NOV) ||

               is_xmas ()));
}

// ----------------------------------------------------------------------------

// Is Veteran's Day a bank holiday?
//
bool DateTime::is_us_bank_holiday () const noexcept  {

    if (! is_us_business_day ())
        return (true);
    else  {
        const DatePartType  m_day = dmonth (); // 1 - 31
        const DT_MONTH      mon = month ();    // JAN - DEC
        const DT_WEEKDAY    w_day = dweek ();  // SUN - SAT

        // Columbus Day (second Monday of October)
        //
        if (w_day == DT_WEEKDAY::MON &&
            m_day >= 8 &&
            m_day <= 14 &&
            mon == DT_MONTH::OCT)
            return (true);
    }

    return (false);
}

// ----------------------------------------------------------------------------

void DateTime::set_timezone (DT_TIME_ZONE tz)  {

    const EpochType t = this->time ();

    time_zone_ = tz;
    breaktime_ (t, nanosec ());
    return;
}

// ----------------------------------------------------------------------------

DT_TIME_ZONE DateTime::get_timezone () const  { return (time_zone_); }

// ----------------------------------------------------------------------------

DateTime::DateType DateTime::date () const noexcept  {

    if (date_ == INVALID_DATE_)
        const_cast<DateTime *>(this)->breaktime_ (this->time (), nanosec ());

    return (date_);
}

// ----------------------------------------------------------------------------

DateTime::DatePartType DateTime::year () const noexcept  {

    return (date () / 10000);
}

// ----------------------------------------------------------------------------

DT_MONTH DateTime::month () const noexcept  {

    return (static_cast<DT_MONTH>((date () % 10000) / 100));
}

// ----------------------------------------------------------------------------

DateTime::DatePartType DateTime::dmonth () const noexcept  {

    return (date () % 100);
}

// ----------------------------------------------------------------------------

DateTime::DatePartType DateTime::dyear () const noexcept  {

    struct tm   ltime;

    // It _always_ makes me sad to use const_cast<>. But then I get
    // over it.
    //
    const_cast<DateTime *>(this)->time_ = maketime_ (ltime);

    return (ltime.tm_yday + 1);
}

// ----------------------------------------------------------------------------

DT_WEEKDAY DateTime::dweek () const noexcept  {

    // It _always_ makes me sad to use const_cast<>. But then I get
    // over it.
    //
    if (week_day_ == DT_WEEKDAY::BAD_DAY)
        const_cast<DateTime *>(this)->breaktime_ (this->time (), nanosec ());

    return (week_day_);
}

// ----------------------------------------------------------------------------

DateTime::HourType DateTime::hour () const noexcept  {

    // It _always_ makes me sad to use const_cast<>. But then I get
    // over it.
    //
    if (hour_ == INVALID_HOUR_)
        const_cast<DateTime *>(this)->breaktime_ (this->time (), nanosec ());
     return (hour_);
}

// ----------------------------------------------------------------------------

DateTime::MinuteType DateTime::minute () const noexcept  {

    if (minute_ == INVALID_MINUTE_)
        const_cast<DateTime *>(this)->breaktime_ (this->time (), nanosec ());

    return (minute_);
}

// ----------------------------------------------------------------------------

DateTime::SecondType DateTime::sec () const noexcept  {

    if (second_ == INVALID_SECOND_)
        const_cast<DateTime *>(this)->breaktime_ (this->time (), nanosec ());

    return (second_);
}

// ----------------------------------------------------------------------------

DateTime::MillisecondType DateTime::msec () const noexcept  {

    const double    slug = double (nanosec ()) / 1000000.0 + 0.5;

    return(static_cast<MillisecondType>(slug < 1000.0 ? slug : 999.0));
}

// ----------------------------------------------------------------------------

DateTime::MicrosecondType DateTime::microsec () const noexcept  {

    const double    slug = double (nanosec ()) / 1000.0 + 0.5;

    return (static_cast<MicrosecondType>(slug < 1000000.0 ? slug : 999999.0));
}

// ----------------------------------------------------------------------------

DateTime::NanosecondType DateTime::nanosec () const noexcept  {

    // nano secs could be negative because of architecture
    return (nanosecond_);
}

// ----------------------------------------------------------------------------

// The reason for time_ being INVALID_TIME_T_ is performance. sometimes
// time_ has to be calculated by mktime() which is very expensive. In
// those cases we assign INVALID_TIME_T_ to time_ and whenever, it
// becomes necessary to calculate time_ (i.e. below), we do it only
// then.
//
DateTime::EpochType DateTime::time () const noexcept  {

    if (time_ == INVALID_TIME_T_)  {
        struct tm   ltime;

        // It _always_ makes me sad to use const_cast<>. But then I get
        // over it.
        //
        const_cast<DateTime *>(this)->time_ = maketime_ (ltime);
    }

    return (time_);
}

// ----------------------------------------------------------------------------

DateTime::LongTimeType DateTime::long_time () const noexcept  {

    return (static_cast<LongTimeType>(this->time()) * 1000000000LL +
            static_cast<LongTimeType>(nanosec()));
}

// ----------------------------------------------------------------------------

double DateTime::diff_seconds (const DateTime &that) const  {

    // Currently I don't have time to implement this. There are
    // more important things to do. But we should do it at some
    // point.
    //
    if (time_zone_ != that.time_zone_)
        throw std::runtime_error (
            "DateTime::diff_seconds(): "
            "Time difference between different time zones "
            "is not implemented currently.");

    const double    this_time =
        static_cast<double>(this->time ()) +
        (static_cast<double>(nanosec ()) / 1000000000.0);
    const double    that_time =
        static_cast<double>(that.time ()) +
        (static_cast<double>(that.nanosec ()) / 1000000000.0);

    return (this_time - that_time);
}

// ----------------------------------------------------------------------------

double DateTime::diff_minutes (const DateTime &that) const noexcept  {

    return (diff_seconds (that) / 60.0);
}

// ----------------------------------------------------------------------------

double DateTime::diff_hours (const DateTime &that) const noexcept  {

    return (diff_minutes (that) / 60.0);
}

// ----------------------------------------------------------------------------

double DateTime::diff_days (const DateTime &that) const noexcept  {

    return (diff_hours (that) / 24.0);
}

// ----------------------------------------------------------------------------

double DateTime::diff_weekdays (const DateTime &that) const noexcept  {

    const int   addend = compare(that) ? -1 : 1;
    DateTime    slug (that);
    double      ddays = 0.0;

    while (slug.date () != date ())  {
        if (! slug.is_weekend ())
            ddays += static_cast<const double>(addend);
        slug.add_days (addend);
    }

    return (ddays + (diff_seconds (slug) / static_cast<double>(24 * 3600)));
}

// ----------------------------------------------------------------------------

double DateTime::diff_weeks (const DateTime &that) const noexcept  {

    return (diff_days (that) / 7.0);
}

// ----------------------------------------------------------------------------

void DateTime::add_seconds (EpochType secs) noexcept  {

    set_time (this->time () + secs, nanosec ());
    return;
}

// ----------------------------------------------------------------------------

void DateTime::add_nanoseconds (long nanosecs) noexcept  {

    long long int   new_time =
        static_cast<long long int>(this->time()) * 1000000000LL +
        static_cast<long long int>(nanosec());

    new_time += static_cast<long long int>(nanosecs);
    set_time(static_cast<EpochType>(new_time / 1000000000LL),
             static_cast<NanosecondType>(new_time % 1000000000LL));
    return;
}

// ----------------------------------------------------------------------------

void DateTime::add_days (long days) noexcept  {

    if (days != 0)  {
        const int   addend = days < 0 ? -1 : 1;

        while (days)  {
            const DateTime  prev_date (*this);

            add_seconds (addend * 24 * 3600);

            // Take care of DST vs EST:
            // Ask me about why I have while loops
            //
            if (date () == prev_date.date ())  {
                while (date () == prev_date.date ())
                    add_seconds (3600 * addend);
            }
            else if (addend > 0 && int(dyear()) - int(prev_date.dyear()) > 1) {
                while (int(dyear ()) - int(prev_date.dyear ()) > 1)
                    add_seconds (-3600 * addend);
            }
            else if (addend < 0 && int(dyear()) - int(prev_date.dyear()) < -1) {
                while (int(dyear ()) - int(prev_date.dyear()) < -1)
                    add_seconds (3600);
            }
            else if (hour () < prev_date.hour ())  {
                while (hour () < prev_date.hour ())
                    add_seconds (3600);
            }
            else if (hour () > prev_date.hour ())  {
                while (hour () > prev_date.hour ())
                    add_seconds (-3600);
            }

            days -= addend;
        }
    }

    return;
}

// ----------------------------------------------------------------------------

void DateTime::add_weekdays (long days) noexcept  {

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

// ----------------------------------------------------------------------------

void DateTime::add_months (long months) noexcept  {

    int         y = year();
    int         m = static_cast<int>(month());
    const int   addend = months >= 0 ? 1 : -1;

    while (months)  {
        m += addend;
        if (m > 12 || m == 0)  {
            m = m > 12 ? 1 : 12;
            y += addend;
        }
        months -= addend;
    }

    int         new_day = dmonth();
    const int   days_max = days_in_month_(static_cast<DT_MONTH>(m), y);

    if (new_day > days_max)
        new_day = days_max;

    const DateTime  new_di((y * 100 + m) * 100 + new_day,
                           hour(),
                           minute(),
                           sec(),
                           nanosec(),
                           get_timezone());

    *this = new_di;
    return;
}

// ----------------------------------------------------------------------------

void DateTime::add_years (long years) noexcept  {

    int         new_year = year() + years;
    int         new_day = dmonth();
    const int   days_max = days_in_month_(month(), new_year);

    if (new_day > days_max)
        new_day = days_max;

    const DateTime  new_di(
        (new_year * 100 + static_cast<int>(month())) * 100 + new_day,
        hour(),
        minute(),
        sec(),
        nanosec(),
        get_timezone());

    *this = new_di;
    return;
}

// ----------------------------------------------------------------------------

bool DateTime::is_weekend () const noexcept  {

    const DT_WEEKDAY    w_day = dweek ();

    return (w_day == DT_WEEKDAY::SUN || w_day == DT_WEEKDAY::SAT);
}

// ----------------------------------------------------------------------------

bool DateTime::is_newyear () const noexcept  {

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

// ----------------------------------------------------------------------------

bool DateTime::is_xmas () const noexcept  {

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

// ----------------------------------------------------------------------------

std::string DateTime::string_format (DT_FORMAT format) const  {

    std::string  result;

    date_to_str (format, result);
    return (result);
}

// ----------------------------------------------------------------------------

inline void DateTime::change_env_timezone_(DT_TIME_ZONE time_zone)  {

    if (time_zone != DT_TIME_ZONE::LOCAL)  {
#ifdef _MSC_VER
        // SetEnvironmentVariable (L"TZ", TIMEZONES_ [time_zone]);
        _putenv (TIMEZONES_[static_cast<int>(time_zone)]);
        _tzset ();
#else
        ::setenv ("TZ", TIMEZONES_[static_cast<int>(time_zone)], 1);
        ::tzset ();
#endif // _MSC_VER
    }
}

// ----------------------------------------------------------------------------

inline void DateTime::reset_env_timezone_(DT_TIME_ZONE time_zone)  {

    if (time_zone != DT_TIME_ZONE::LOCAL)  {
#ifdef _MSC_VER
        // SetEnvironmentVariable (L"TZ", nullptr);
        _putenv ("TZ=");
        _tzset ();
#else
        ::unsetenv ("TZ");
        ::tzset ();
#endif // _MSC_VER
    }
}

// ----------------------------------------------------------------------------

DateTime::EpochType DateTime::maketime_ (struct tm &ltime) const noexcept  {

    ltime.tm_sec = sec ();
    ltime.tm_isdst = -1;
    ltime.tm_min = minute ();
    ltime.tm_hour = hour ();
    ltime.tm_mday = dmonth ();
    ltime.tm_mon = static_cast<int>(month()) - 1;
    ltime.tm_year = year () - 1900;

    change_env_timezone_(time_zone_);

    const time_t    t  = ::mktime (&ltime);

    reset_env_timezone_(time_zone_);
    return (t);
}

// ----------------------------------------------------------------------------

void
DateTime::breaktime_ (EpochType the_time, NanosecondType nanosec) noexcept  {

    change_env_timezone_(time_zone_);

    struct tm   ltime;

#ifdef _MSC_VER
    localtime_s (&ltime, &the_time);
#else
    localtime_r (&the_time, &ltime);
#endif // _MSC_VER

    reset_env_timezone_(time_zone_);

    date_ = (ltime.tm_year + 1900) * 100;

    date_ += (ltime.tm_mon + 1);
    date_ *= 100;
    date_ += ltime.tm_mday;

    hour_ = ltime.tm_hour;
    minute_ = ltime.tm_min;
    second_ = ltime.tm_sec;
    nanosecond_ = nanosec;
    time_ = the_time;
    week_day_ = static_cast<DT_WEEKDAY>(ltime.tm_wday + 1);

    return;
}

// ----------------------------------------------------------------------------

template<typename T>
void DateTime::date_to_str (DT_FORMAT format, T &result) const  {

    String128   buffer;

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

        case DT_FORMAT::ISO_DT_TM:
        {
            buffer.printf ("%d-%002d-%002d %002d:%002d:%002d.%0000006d",
                           static_cast<int>(year ()),
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()),
                           static_cast<int>(hour ()),
                           static_cast<int>(minute ()),
                           static_cast<int>(sec ()),
                           static_cast<int>(microsec ()));
        } break;

        case DT_FORMAT::ISO_DT:
        {
            buffer.printf ("%d-%002d-%002d",
                           static_cast<int>(year ()),
                           static_cast<int>(month ()),
                           static_cast<int>(dmonth ()));
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

        case DT_FORMAT::DT_PRECISE:  // e.g. Epoch.Nanoseconds
        {
            buffer.printf ("%ld.%d", this->time(), nanosec());
        } break;

        default:
        {
            buffer.printf ("ERROR: DateTime::date_to_str(): "
                           "Unknown format: '%u'",
                           format);
            throw std::runtime_error (buffer.c_str ());
        }
    }

    result = buffer.c_str ();
    return;
}

} // namespace hmdf

// ----------------------------------------------------------------------------

// Local Variables:
// mode:C++
// tab-width:4
// c-basic-offset:4
// End:
