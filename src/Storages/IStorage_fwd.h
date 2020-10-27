#pragma once

#include <common/types.h>

#include <map>
#include <memory>
#include <common/logger_useful.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class IStorage;

using BaseStoragePtr = std::shared_ptr<IStorage>;

class KekPtr : public BaseStoragePtr
{
public:
    KekPtr() = default;
    KekPtr(IStorage * ) {}
    template<typename T>    //const_cast<IStorage *>(static_cast<const IStorage *>(sptr.get()))
    KekPtr(const std::shared_ptr<T> & sptr) : BaseStoragePtr(std::const_pointer_cast<IStorage>(std::static_pointer_cast<const IStorage>(sptr))) { logTrace("created", __PRETTY_FUNCTION__ ); }
    template<typename T>
    KekPtr(std::shared_ptr<T> && sptr) : BaseStoragePtr(std::const_pointer_cast<IStorage>(std::static_pointer_cast<const IStorage>(std::move(sptr))))  { logTrace("created (moved)", __PRETTY_FUNCTION__ ); }

    ~KekPtr() { logTrace("dtor", __PRETTY_FUNCTION__); }

    KekPtr(const KekPtr & sptr) : BaseStoragePtr(sptr) { logTrace("created2", __PRETTY_FUNCTION__ ); }
    KekPtr(KekPtr && sptr) : BaseStoragePtr(std::move(sptr))  { logTrace("created2 (moved)", __PRETTY_FUNCTION__ ); }
    KekPtr & operator = (const KekPtr & sptr)
    {
        logTrace("will assign", __PRETTY_FUNCTION__ );
        BaseStoragePtr::operator=(sptr);
        logTrace("assigned", __PRETTY_FUNCTION__ );
        return *this;
    }

    KekPtr & operator = (KekPtr && sptr)
    {
        logTrace("will assign (move)", __PRETTY_FUNCTION__ );
        BaseStoragePtr::operator=(sptr);
        logTrace("assigned (moved)", __PRETTY_FUNCTION__ );
        return *this;
    }

    void logTrace(const String & desc, const String & func) const noexcept;

    static Poco::Logger * getLogger() { return &Poco::Logger::get("StoragePtrWrapper"); }

    //operator std::shared_ptr<IStorage> () { return ptr; }
    //operator bool () { return ptr.get(); }
    //IStorage & operator*() const noexcept { return *ptr; }
    //IStorage * operator->() const noexcept { return ptr.get(); }
    //IStorage * get() const noexcept { return ptr.get(); }
    //void reset() noexcept { ptr.reset(); }
    //long use_count() const noexcept { return ptr.use_count(); }
    //bool unique() const noexcept {return ptr.unique(); }
private:
    //std::shared_ptr<IStorage> ptr;
};

using ConstStoragePtr = KekPtr;
using StoragePtr = KekPtr;
using Tables = std::map<String, StoragePtr>;

}
