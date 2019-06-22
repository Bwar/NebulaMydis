/*******************************************************************************
 * Project:  NebulaMydis
 * @file     DbStorageStep.hpp
 * @brief 
 * @author   Bwar
 * @date:    2018年3月2日
 * @note
 * Modify history:
 ******************************************************************************/
#ifndef MYDIS_DBSTORAGESTEP_HPP_
#define MYDIS_DBSTORAGESTEP_HPP_

#include "actor/step/PbStep.hpp"
#include "actor/context/PbContext.hpp"
#include "pb/mydis.pb.h"
#include "SessionRedisNode.hpp"

namespace mydis
{

class DbStorageStep: public neb::PbStep
{
public:
    DbStorageStep(std::shared_ptr<neb::Step> pNextStep = nullptr);
    virtual ~DbStorageStep();

protected:
    bool Response(int iErrno, const std::string& strErrMsg);
    bool Response(const neb::Result& oRsp);
};

}/* namespace mydis */

#endif /* MYDIS_DBSTORAGESTEP_HPP_ */

