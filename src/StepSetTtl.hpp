/*******************************************************************************
 * Project:  NebulaMydis
 * @file     StepSetTtl.hpp
 * @brief    设置过期时间
 * @author   Bwar
 * @date:    2018年3月3日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_STEPSETTTL_HPP_
#define MYDIS_STEPSETTTL_HPP_

#include "RedisStorageStep.hpp"

namespace mydis
{

class StepSetTtl: public RedisStorageStep,
    public neb::DynamicCreator<StepSetTtl, std::string, std::string, int32>
{
public:
    StepSetTtl(const std::string& strMasterNodeIdentify, const std::string& strKey, int32 iExpireSeconds);
    virtual ~StepSetTtl();

    virtual neb::E_CMD_STATUS Emit(int iErrno, const std::string& strErrMsg = "", void* data = NULL);

    virtual neb::E_CMD_STATUS Callback(
                    const redisAsyncContext *c,
                    int status,
                    redisReply* pReply);

private:
    std::string m_strMasterNodeIdentify;
    std::string m_strKey;
    int32 m_iExpireSeconds;
};

} /* namespace mydis */

#endif /* MYDIS_STEPSETTTL_HPP_ */
