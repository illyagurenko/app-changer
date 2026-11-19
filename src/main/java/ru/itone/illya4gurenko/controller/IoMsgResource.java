package ru.itone.illya4gurenko.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import ru.itone.illya4gurenko.dao.IoMsgsDao;
import ru.itone.illya4gurenko.dto.IoMsgDto;

/**
 * rest-контроллер для получения записей аудита APP_CHANGER_IO_MSGS
 */
@Path("/api/io-msgs")
@Produces(MediaType.APPLICATION_JSON)
public class IoMsgResource {

    @Inject
    IoMsgsDao ioMsgsDao;

    /**
     * возвращает dto последней записи из APP_CHANGER_IO_MSGS (с опциональным фильтром dir=IN|OUT)
     */
    @GET
    @Path("/latest")
    public Response getLatest(@QueryParam("dir") String dir) {
        IoMsgDto dto = ioMsgsDao.findLatest(dir);
        if (dto == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(dto).build();
    }

    /**
     * возвращает dto записи из APP_CHANGER_IO_MSGS по requestId и направлению (IN / OUT)
     */
    @GET
    @Path("/by-request")
    public Response getByRequestId(@QueryParam("requestId") String requestId,
                                   @QueryParam("dir") String dir) {
        if (requestId == null || requestId.isBlank() || dir == null || dir.isBlank()) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        IoMsgDto dto = ioMsgsDao.findByRequestIdAndDir(requestId, dir);
        if (dto == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(dto).build();
    }
}
