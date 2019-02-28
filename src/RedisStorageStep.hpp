/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StorageStep.hpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月2日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_REDISSTORAGESTEP_HPP_
#define MYDIS_REDISSTORAGESTEP_HPP_

#include "channel/SocketChannel.hpp"
#include "actor/step/RedisStep.hpp"
#include "actor/session/Context.hpp"
#include "pb/mydis.pb.h"
#include "SessionRedisNode.hpp"

namespace mydis
{

/**
 * @brief redis数据结构与DB表数据之间关系
 */
enum E_TABLE_RELATIVE
{
    RELATIVE_TABLE      = 0,    //!< RELATIVE_TABLE   redis hash与表字段一一对应
    RELATIVE_DATASET    = 1,    //!< RELATIVE_DATASET redis数据为表的各字段值序列化成record
    RELATIVE_JOIN       = 2,    //!< RELATIVE_JOIN    redis数据为表的某些字段用冒号“:”连接而成
};

enum E_REDIS_TYPE
{
    REDIS_T_HASH                = 1,    ///< redis hash
    REDIS_T_SET                 = 2,    ///< redis set
    REDIS_T_KEYS                = 3,    ///< redis keys
    REDIS_T_STRING              = 4,    ///< redis string
    REDIS_T_LIST                = 5,    ///< redis list
    REDIS_T_SORT_SET            = 6,    ///< redis sort set
};

class RedisStorageStep: public neb::RedisStep
{
public:
    RedisStorageStep(std::shared_ptr<neb::Step> pNextStep = nullptr);
    RedisStorageStep(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq, std::shared_ptr<neb::Step> pNextStep = nullptr);
    RedisStorageStep(std::shared_ptr<neb::SocketChannel> pChannel, int32 iCmd, uint32 uiSeq, const MsgBody& oReqMsgBody, std::shared_ptr<neb::Step> pNextStep = nullptr);
    virtual ~RedisStorageStep();

protected:
    bool Response(int iErrno, const std::string& strErrMsg);
    bool Response(const neb::Result& oRsp);
};

} /* namespace mydis */

#endif /* MYDIS_REDISSTORAGESTEP_HPP_ */
