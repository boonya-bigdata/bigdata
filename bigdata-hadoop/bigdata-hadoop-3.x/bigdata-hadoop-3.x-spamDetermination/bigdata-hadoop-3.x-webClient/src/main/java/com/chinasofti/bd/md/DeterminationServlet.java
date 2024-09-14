package com.chinasofti.bd.md;

import com.chinasofti.platform.rpc.Service;
import com.chinasofti.spamdetermination.rpcserver.bizinterface.ISpamDeterminationBiz;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Servlet implementation class DeterminationServlet
 */
public class DeterminationServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	ISpamDeterminationBiz biz = (ISpamDeterminationBiz) Service.lookup(
			ServerContext.COUNTER_SERVER, "service");

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public DeterminationServlet() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		request.setCharacterEncoding("utf-8");
		response.setCharacterEncoding("utf-8");
		String msg = request.getParameter("msg");
		request.setAttribute("msg", msg);
		if (biz.isSpam(msg)) {
			request.setAttribute("result", "垃圾信息");
		} else {
			request.setAttribute("result", "有效信息");
		}
		request.getRequestDispatcher("/result.jsp").forward(request, response);

	}
	
	
	

}
